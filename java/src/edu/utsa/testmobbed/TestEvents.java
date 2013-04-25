package edu.utsa.testmobbed;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.*;
import edu.utsa.testmobbed.helpers.Datasets;

public class TestEvents {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "eventdb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = true;
	private static ManageDB md;
	private static Datasets dataset;
	private static Events urevent;
	private static Events event;

	@Before
	public void setup() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
			boolean isUnique = false;
			String datasetName = "ELEMENT_TEST";
			String datasetContactUuid = "691df7dd-ce3e-47f8-bea5-6a632c6fcccb";
			String datasetDescription = "Elements test";
			String datasetModalityUuid = "791df7dd-ce3e-47f8-bea5-6a632c6fcccb";
			String datasetNameSpace = "ELEMENTS_TEST";
			String datasetParentUuid = null;
			dataset = new Datasets(md.getConnection());
			dataset.reset(isUnique, datasetName, datasetContactUuid,
					datasetDescription, datasetNameSpace, datasetModalityUuid,
					datasetParentUuid);
			dataset.save();
			String datasetUuid = dataset.getDatasetUuid().toString();
			String urelementField = "urevent";
			String elementField = "event";
			String[] eventTypes = { "et1", "et2" };
			long[] positions = { 1, 2 };
			double[] eventLatencies = { 111, 222 };
			double[] eventCertainties = { 1.0, 1.0 };
			String[] ureventParents = { ManageDB.noParentUuid,
					ManageDB.noParentUuid };
			urevent = new Events(md.getConnection());
			urevent.reset(datasetUuid, urelementField, eventTypes, eventTypes,
					positions, eventLatencies, eventLatencies,
					eventCertainties, null, ureventParents);
			String[] eventParents = urevent.addEvents();
			event = new Events(md.getConnection());
			event.reset(datasetUuid, elementField, eventTypes, eventTypes,
					positions, eventLatencies, eventLatencies,
					eventCertainties, null, eventParents);
		}

	}

	@After
	public void cleanup() throws Exception {
		Statement stmt = md.getConnection().createStatement();
		String query = "DELETE FROM EVENTS";
		stmt.execute(query);
		md.close();
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testAddAttribute() throws Exception {
		System.out.println("TEST: testing addAttribute() method.");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM ATTRIBUTES";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("-- There should be no attributes in the database.");
		assertEquals("There are attributes in the database", expected, actual);
		String fieldName = "urevent";
		Double[] numAttrValues = { 1.0, 2.0 };
		String[] attrValues = { "1.0", "2.0" };
		urevent.save();
		event.addEvents();
		event.addAttribute(fieldName, numAttrValues, attrValues);
		event.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 2;
		actual = rs.getInt(1);
		System.out.println("-- There should be 2 attributes in the database.");
		assertEquals("There are no attributes in the database", expected,
				actual);

	}

	@Test
	public void testAddEvents() throws Exception {
		System.out.println("TEST: testing addEvents() method.");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM EVENTS";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("-- There should be no events in the database.");
		assertEquals("There are events in the database", expected, actual);
		urevent.save();
		event.addEvents();
		event.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 4;
		actual = rs.getInt(1);
		System.out.println("-- There should be 4 events in the database.");
		assertEquals("There are no events in the database", expected, actual);
	}

}
