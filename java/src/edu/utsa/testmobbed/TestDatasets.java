package edu.utsa.testmobbed;

import static org.junit.Assert.assertTrue;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.*;
import edu.utsa.testmobbed.helpers.Datasets;


public class TestDatasets {
	private String tablePath = Class.class
			.getResource("/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose;
	private ManageDB md;

	@Before
	public void setUp() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password,
					tablePath, verbose);
			md = new ManageDB(name, hostname, user, password);
		}
	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test(expected = MobbedException.class)
	public void testRetrieveLatestVersion() throws Exception {

		Datasets dataset1 = new Datasets(md.getConnection());
		dataset1.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		System.out
				.println("TEST: testing retrieveLatestVersion. Version should be zero");
		int version = dataset1.retrieveLatesetVersion();
		dataset1.save();

		System.out
				.println("TEST: testing retrieveLatestVersion. Version should be incremented");
		int versionAfter = dataset1.retrieveLatesetVersion();
		assertTrue("Version did not increase after insert.",
				versionAfter == version + 1);

		System.out
				.println("TEST: testing retrieveLatestVersion. Exception should be thrown");
		Datasets dataset2 = new Datasets(md.getConnection());
		dataset2.reset(true, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE",
				"791df7dd-ce3e-47f8-bea5-6a632c6fcccb", null);
	}

}
