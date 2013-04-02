package edu.utsa.testmobbed;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.*;
import edu.utsa.testmobbed.helpers.Datasets;

public class TestEvents {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose;
	private ManageDB md;
	private Datasets dataset1;
	private Events event1;

	@Before
	public void setUp() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password);
		} finally {
			dataset1 = new Datasets(md.getConnection());
			dataset1.reset(false, "EVENT_DATASET",
					"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
					"DATASET FOR EVENTS", "EVENT_NAMESPACE",
					"791df7dd-ce3e-47f8-bea5-6a632c6fcccb", null);
			dataset1.save();
			long[] positions = new long[] { 1, 2 };
			String[] eventTypes = { "evType1", "evType2" };
			double[] eventLatencies = { 111, 222 };
			double[] eventCertainties = { 1.0, 1.0 };
			String[] defaultFields = {};
			event1 = new Events(md.getConnection());
			event1.reset(dataset1.getDatasetUuid().toString(), "event",
					defaultFields, eventTypes, eventTypes, positions,
					eventLatencies, eventLatencies, eventCertainties, null);
		}

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testAddAttributes() throws Exception {
		String fieldName = "position";
		Double[] numAttrValues = { 1.0, 2.0 };
		String[] attrValues = { "1.0", "2.0" };

		event1.addEvents();
		event1.addAttribute(fieldName, numAttrValues, attrValues);
		event1.save();
	}

	@Test
	public void testAddEvents() throws Exception {
		event1.addEvents();
		event1.save();
	}

	@Test
	public void testAddFields() throws Exception {
		event1.addEvents();

		String fieldName1 = "position";
		Double[] numAttrValues1 = { 1.0, 2.0 };
		String[] attrValues1 = { "1.0", "2.0" };

		event1.addAttribute(fieldName1, numAttrValues1, attrValues1);

		String fieldName2 = "urevent";
		Double[] numAttrValues2 = { 1.0, 2.0 };
		String[] attrValues2 = { "1.0", "2.0" };
		event1.addAttribute(fieldName2, numAttrValues2, attrValues2);
		event1.save();
	}

}
