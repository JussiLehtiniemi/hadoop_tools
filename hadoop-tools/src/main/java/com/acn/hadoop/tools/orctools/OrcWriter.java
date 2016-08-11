package com.acn.hadoop.tools.orctools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcWriter {
	
	private static final  Logger LOG = LoggerFactory.getLogger(OrcWriter.class);
	
	public static final long MIB = 1048576L;
	
	private File inputFile;
	private String outputPath;
	
	public OrcWriter() {
		
	}
	
	public void setInputFile(File file) {
		this.inputFile = file;
	}
	
	public void setOutputPath(String path) {
		this.outputPath = path;
	}
	
	public void writeOrcFile() throws IllegalArgumentException, FileNotFoundException, IOException {
		
		// Check setup
		if(this.inputFile == null) {
			LOG.error("Input file is null!");
			throw new IllegalArgumentException("Input file is null!");
		}
		
		if(this.outputPath == null) {
			LOG.error("Output path is null!");
			throw new IllegalArgumentException("Output path is null!");
		}
		
		// Write the ORC file
		
		LOG.info("Starting to write ORC file...");
		
		InputStream in = null;
		InputStreamReader isRdr = null;
		BufferedReader reader = null;
		Writer writer = null;
		
		try {
			// Read input file
			in = new FileInputStream(inputFile);
			isRdr = new InputStreamReader(in, Charset.forName("UTF-8"));
			reader = new BufferedReader(isRdr);
			
			// Define file structure
			String typeStr = "struct<eventTime:timestamp,level:string,source:string,message:string>";
			
			// Set up ORC writer
			TypeDescription schema = TypeDescription.fromString(typeStr);
			
			Configuration conf = new Configuration();
			Path path = new Path(this.outputPath);
			
			WriterOptions opts = OrcFile.writerOptions(conf)
					.blockSize(64 * MIB)
					.stripeSize(64 * MIB)
					.bufferSize((int) MIB)
					.setSchema(schema);
			
			writer = OrcFile.createWriter(path, opts);

			// Write the ORC file
			
			VectorizedRowBatch batch = schema.createRowBatch();
			
			TimestampColumnVector ts = (TimestampColumnVector) batch.cols[0];
			BytesColumnVector level = (BytesColumnVector) batch.cols[1];
			BytesColumnVector source = (BytesColumnVector) batch.cols[2];
			BytesColumnVector msg = (BytesColumnVector) batch.cols[3];
			
			String line = "";
			
			while((line = reader.readLine()) != null) {
				
				String[] tokens = line.split("\\|");
				
				// Write ORC file in row batches
				
				int row = batch.size++;
				
				ts.set(row, new OrcTimestamp(tokens[0]));
				level.setVal(row, tokens[1].getBytes(Charset.forName("UTF-8")));
				source.setVal(row, tokens[2].getBytes(Charset.forName("UTF-8")));
				msg.setVal(row, tokens[3].getBytes(Charset.forName("UTF-8")));
				
				// Check if batch is full, then write to ORC
				if(batch.size == batch.getMaxSize()) {
					writer.addRowBatch(batch);
					batch.reset();
				}
			}
			
			// Write last batch
			if(batch.size > 0) {
				writer.addRowBatch(batch);
			}
			
			writer.close();
		}
		catch(IOException e) {
			LOG.error("Error while writing ORC file!", e);
			throw e;
		}
		finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(isRdr);
			IOUtils.closeQuietly(reader);
			try {
				if(writer != null)
					writer.close();
			}
			catch(Exception e) {}
		}
	}

	
}
