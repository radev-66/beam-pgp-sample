package com.arvi.samples;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.NoSuchProviderException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.bouncycastle.openpgp.PGPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arvi.samples.pgp.PGPDecrypt;

@SuppressWarnings("serial")
public class ProcessEncryptedFile {

	private static final Logger logger = LoggerFactory.getLogger(ProcessEncryptedFile.class);

	private static final class ReadFromEncryptedFile extends DoFn<ReadableFile, String> {
		
		
		private String privateKeyPath;
		private String privateKeyPasswd;


		
		

		public ReadFromEncryptedFile(String privateKeyPath, String privateKeyPasswd) {
			super();
			this.privateKeyPath = privateKeyPath;
			this.privateKeyPasswd = privateKeyPasswd;
		}





		@ProcessElement
		public void processElement(@Element ReadableFile input, OutputReceiver<String> receiver) {
	

			PGPDecrypt decrypt = new PGPDecrypt();
			InputStream decryptedInputStream;



			try (ReadableByteChannel open = input.open();
					InputStream newInputStream = Channels.newInputStream(open)) {
				
				ResourceId existingFileResourceId = FileSystems
					    .matchSingleFileSpec(this.privateKeyPath)
					    .resourceId();

				
			    try (ReadableByteChannel keyFileChannel = FileSystems.open(existingFileResourceId);					
			    InputStream keyIn = new BufferedInputStream(Channels.newInputStream(keyFileChannel))) {
				
					decryptedInputStream = decrypt.getDecryptedInputStream(newInputStream, keyIn, this.privateKeyPasswd.toCharArray());
					BufferedReader reader = new BufferedReader(new InputStreamReader(decryptedInputStream));
	
					String line = null;
					while ((line = reader.readLine()) != null) {
						receiver.output(line);
					}
			    }

			} catch (NoSuchProviderException | PGPException | IOException e) {
				logger.error("Error reading or decrypting file: {}", input.getMetadata().resourceId().getFilename());
				logger.error("Exception Decrypting ", e);
				e.printStackTrace();
				throw new RuntimeException(e);
			}

		}
	}

	private static final class PrintString extends SimpleFunction<String, Void> {
		@Override
		public Void apply(String input) {
			System.out.println(input);
			return null;
		}
	}

	public interface ProcessEncryptedFileOptions extends PipelineOptions {

		void setInputFilePattern(String value);

		@Description("Path of the file to read from")
		//@Default.String("gs://encrypted-files/*.out")
		@Required
		String getInputFilePattern();

		/** Set this required option to specify where to write the output. */
		@Description("Path of the file to write to")
		@Required
		String getOutput();

		void setOutput(String value);
		
		@Description("Path of the private key file to read from")
		@Required
		String getPrivateKeyPath();

		
		void setPrivateKeyPath(String value);
		
		void setPrivateKeyPasswd(String value);
		
		@Required
		String getPrivateKeyPasswd();

	}

	@SuppressWarnings("unused")
	private static final class ReadAsString extends SimpleFunction<ReadableFile, String> {
		@Override
		public String apply(ReadableFile input) {

			ReadableByteChannel open;
			try {
				open = input.open();

				StringBuilder builder = new StringBuilder();
				Reader newReader = Channels.newReader(open, "UTF-8");
				BufferedReader reader = new BufferedReader(newReader);

				String line = null;
				while ((line = reader.readLine()) != null) {
					builder.append(line);
				}

				return builder.toString();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}
	}

	private static final class PGPDecryptAsStream extends SimpleFunction<ReadableFile, String> {
		

		private String privateKeyPath;
		private String privateKeyPasswd;

		public PGPDecryptAsStream(String privateKeyPath,  String privateKeyPasswd) {
			this.privateKeyPath = privateKeyPath;
			this.privateKeyPasswd = privateKeyPasswd;
		}

		@Override
		public String apply(ReadableFile input) {

			PGPDecrypt decrypt = new PGPDecrypt();
			InputStream decryptedInputStream;
			try (ReadableByteChannel open = input.open();
					InputStream newInputStream = Channels.newInputStream(open);
					InputStream keyIn = new BufferedInputStream(
							new FileInputStream(privateKeyPath));) {

				decryptedInputStream = decrypt.getDecryptedInputStream(newInputStream, keyIn, privateKeyPasswd.toCharArray());

				BufferedReader reader = new BufferedReader(new InputStreamReader(decryptedInputStream));

				StringBuilder builder = new StringBuilder();
				String line = null;

				while ((line = reader.readLine()) != null) {
					builder.append(line);
				}

				return builder.toString();
			} catch (NoSuchProviderException | PGPException | IOException e) {
				logger.error("Error reading or decrypting file: {} {} ", input.getMetadata().resourceId().getFilename(), e);
				logger.error("Exception ", e);
				e.printStackTrace();
				throw new RuntimeException(e);
			}

		}
	}

	@SuppressWarnings("unused")
	private static void processSmallFile(ProcessEncryptedFileOptions options) {
		Pipeline p = Pipeline.create();

		p.apply(FileIO.match().filepattern(options.getInputFilePattern())).apply(FileIO.readMatches())
				.apply(MapElements.via(new PGPDecryptAsStream(options.getPrivateKeyPath(), options.getPrivateKeyPasswd()))).apply(MapElements.via(new PrintString()));

		p.run().waitUntilFinish();
	}

	private static void processHugeFile(ProcessEncryptedFileOptions options) {

		Pipeline p = Pipeline.create(options);

		p.apply(FileIO.match().filepattern(options.getInputFilePattern())).apply(FileIO.readMatches())
				.apply(ParDo.of(new ReadFromEncryptedFile(options.getPrivateKeyPath(), options.getPrivateKeyPasswd())))

				.apply(ParDo.of(new DoFn<String, Void>() {

					@ProcessElement
					public void processElement(@Element String input, OutputReceiver<Void> receiver) {
						logger.info("Processed line {}", input);
					}
				}))

				// .apply(TextIO.write().to(options.getOutput()))
				;
		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {
		ProcessEncryptedFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ProcessEncryptedFileOptions.class);
		// processSmallFile();
		processHugeFile(options);
	}

}
