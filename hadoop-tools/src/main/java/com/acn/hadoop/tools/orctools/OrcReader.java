package com.acn.hadoop.tools.orctools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcReader {

	private static final Logger LOG = LoggerFactory.getLogger(OrcReader.class);
	
	private String filePath;
	
	public OrcReader() {}
	
	public void setFilePath(String path) {
		this.filePath = path;
	}
	
	public void readRecords() throws IllegalArgumentException, IOException {
		
		if(this.filePath == null) {
			LOG.error("File path is not set!");
			throw new IllegalArgumentException("File path is not set!");
		}
		
		Reader reader = null;
		RecordReader rows = null;
		
		try {
			
			Configuration conf = new Configuration();
			Path path = new Path(this.filePath);
			
			ReaderOptions opts = OrcFile.readerOptions(conf);
			
			reader = OrcFile.createReader(path, opts);
			
			rows = reader.rows();
			
			VectorizedRowBatch batch = reader.getSchema().createRowBatch();
			
			while(rows.nextBatch(batch)) {
				TimestampColumnVector tstamp = (TimestampColumnVector) batch.cols[0];
				BytesColumnVector level =  (BytesColumnVector) batch.cols[1];
				BytesColumnVector source = (BytesColumnVector) batch.cols[2];
				BytesColumnVector msg = (BytesColumnVector) batch.cols[3];
				
				for(int r = 0; r < batch.size; r++) {
					StringBuilder buf = new StringBuilder();
					
					tstamp.stringifyValue(buf, r);
					buf.append("|");
					buf.append(level.toString(r));
					buf.append("|");
					buf.append(source.toString(r));
					buf.append("|");
					buf.append(msg.toString(r));
					
					LOG.debug(buf.toString());
				}
			}
		}
		catch(IOException e) {
			LOG.error("Error reading ORC file!", e);
			throw e;
		}
		finally {
		if(rows != null)
			rows.close();
		}
	}
}
