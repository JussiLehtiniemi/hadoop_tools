package com.acn.hadoop.tools.orctools.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;

import com.acn.hadoop.tools.orctools.OrcReader;

public class OrcReaderTest {
	
	private static final String TESTFILE = "test/test.orc";

	@Test
	public void testReadRecords() throws Exception {
		
		OrcReader rdr = new OrcReader();
		
		rdr.setFilePath(TESTFILE);
		
		rdr.readRecords();
	}

}
