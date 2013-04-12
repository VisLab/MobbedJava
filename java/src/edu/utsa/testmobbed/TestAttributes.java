/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertTrue;

import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.Attributes;
import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.Structures;
import edu.utsa.testmobbed.helpers.Datasets;

/**
 * @author JCockfield
 * 
 */
public class TestAttributes {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose = true;
	private ManageDB md;
	private Datasets dataset1;
	private Attributes attribute1;
	private Structures structure1;

	@Before
	public void setUp() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			dataset1 = new Datasets(md.getConnection());
			dataset1.reset(false, "ELEMENT_DATASET",
					"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
					"DATASET FOR ELEMENTS", "ELEMENT_NAMESPACE",
					"791df7dd-ce3e-47f8-bea5-6a632c6fcccb", null);
			dataset1.save();
			structure1 = new Structures(md.getConnection());
			structure1.reset(UUID.randomUUID(), "attributeStructure", null);
			structure1.save();
			attribute1 = new Attributes(md.getConnection());
			attribute1.reset(UUID.randomUUID(), UUID.randomUUID(),
					dataset1.getDatasetUuid(), structure1.getStructureUuid(),
					(double) 1, "1");
		}

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testaddToBatch() throws Exception {
		System.out
				.println("TEST: testing addToBatch. There should be no queries in batch");
		int batchCount = attribute1.getBatchCount();
		assertTrue("Initial batch count not 0.", batchCount == 0);

		attribute1.addToBatch();
		batchCount = attribute1.getBatchCount();
		assertTrue("Initial batch count not incremented.", batchCount == 1);
	}

}
