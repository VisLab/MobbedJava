package edu.utsa.testmobbed;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.Elements;
import edu.utsa.mobbed.ManageDB;
import edu.utsa.testmobbed.helpers.Datasets;

public class TestElements {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose;
	private ManageDB md;
	private Datasets dataset1;
	private Elements element1;

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
			dataset1.reset(false, "ELEMENT_DATASET",
					"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
					"DATASET FOR ELEMENTS", "ELEMENT_NAMESPACE",
					"791df7dd-ce3e-47f8-bea5-6a632c6fcccb", null);
			dataset1.save();
			String[] defaultFields = {};
			String[] elementLabels = { "label1", "label2" };
			String[] elementDescriptions = { "EEG channel: 1", "EEG channel: 2" };
			long[] elementPositions = { 1, 2 };
			element1 = new Elements(md.getConnection());
			element1.reset(dataset1.getDatasetUuid().toString(), "chanlocs",
					defaultFields, "EEG Cap", elementLabels,
					elementDescriptions, elementPositions);
		}

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testAddAttributes() throws Exception {
		String fieldName = "X";
		Double[] numAttrValues = { 0.123, 0.456 };
		String[] attrValues = { "0.123", "0.456" };
		element1.addElements();
		element1.addAttribute(fieldName, numAttrValues, attrValues);
		element1.save();
	}

	@Test
	public void testAddElements() throws Exception {
		element1.addElements();
		element1.save();
	}

	@Test
	public void testgetElementCount() throws Exception {
		element1.addElements();
		element1.save();
	}

	@Test
	public void testAddFields() throws Exception {
		element1.addElements();

		String fieldName1 = "X";
		Double[] numAttrValues1 = { 0.123, 0.456 };
		String[] attrValues1 = { "0.123", "0.456" };

		element1.addAttribute(fieldName1, numAttrValues1, attrValues1);

		String fieldName2 = "Y";
		Double[] numAttrValues2 = { 0.123, 0.456 };
		String[] attrValues2 = { "0.123", "0.456" };
		element1.addAttribute(fieldName2, numAttrValues2, attrValues2);
		element1.save();

	}
}
