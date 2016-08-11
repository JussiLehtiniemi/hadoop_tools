package com.acn.hadoop.tools.orctools.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acn.hadoop.tools.orctools.OrcWriter;

public class OrcWriterTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(OrcWriterTest.class);
	
	private static final String TESTFILE = "test/plusEvBetting.log.Apr-2016.preprocess";
	private static final String OUTFILE = "test/out/test_apr-2016.orc";

	@Before
	public void setup() {
		LOG.info("Setting up tests...");
		
		File outfile = new File(OUTFILE);
		
		if(outfile.exists()) {
			LOG.info("Output file exists, deleting...");
			boolean res = outfile.delete();
			if(!res)
				LOG.error("Deleting old output file failed!");
		}
	}
	
	@Test
	public void testWriteOrcFile() throws Exception {
		
		OrcWriter writer = new OrcWriter();
		writer.setInputFile(new File(TESTFILE));
		writer.setOutputPath(OUTFILE);
		
		writer.writeOrcFile();
		
		// Test output
		File outfile = new File(OUTFILE);
		assertThat(outfile.exists(), is(true));
		assertThat(outfile.length(), is(not(0L)));
		
	}

}
