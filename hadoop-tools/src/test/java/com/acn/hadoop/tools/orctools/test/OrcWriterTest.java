package com.acn.hadoop.tools.orctools.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;

import org.junit.Before;

import com.acn.hadoop.tools.orctools.OrcWriter;

import junit.framework.TestCase;

public class OrcWriterTest extends TestCase {
	
	private static final String TESTFILE = "test/plusEvBetting.log.preprocess";
	private static final String OUTFILE = "test/out/test.orc";

	@Before
	public void setup() {
		File outfile = new File(OUTFILE);
		
		if(outfile.exists())
			outfile.delete();
	}
	
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
