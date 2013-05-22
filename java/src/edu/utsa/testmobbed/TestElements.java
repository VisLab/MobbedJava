package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.Elements;
import edu.utsa.mobbed.ManageDB;

/**
 * Unit tests for Elements class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestElements {
	private static String tablePath;
	private static String name = "elementdb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;
	private static Elements element;

	@BeforeClass
	public static void setup() throws Exception {
		try {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
			String datasetValues[][] = { { null, null, null, "ELEMENT_DATASET",
					null, null, null, "ELEMENT DATASET", null, null, null } };
			String[] datasetUuids = md.addRows("datasets",
					md.getColumnNames("datasets"), datasetValues, null, null);
			String[] elementLabels = { "channel 1", "channel 2" };
			String[] elementDescriptions = { "EEG channel: 1", "EEG channel: 2" };
			long[] elementPositions = { 1, 2 };
			element = new Elements(md.getConnection());
			element.reset("EEG", datasetUuids[0], "chanlocs", "EEG CAP",
					elementLabels, elementDescriptions, elementPositions);
		}

	}

	@After
	public void cleanup() throws Exception {
		Statement stmt = md.getConnection().createStatement();
		String query = "DELETE FROM ELEMENTS";
		stmt.execute(query);
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testAddAttribute() throws Exception {
		System.out.println("Unit test for addAttribute");
		System.out.println("It should store 2 element attributes");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM ATTRIBUTES";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("--There should be no attributes in the database.");
		assertEquals("There are attributes in the database", expected, actual);
		String fieldName = "X";
		Double[] numAttrValues = { 0.123, 0.456 };
		String[] attrValues = { "0.123", "0.456" };
		element.addElements();
		element.addAttribute(fieldName, numAttrValues, attrValues);
		element.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 2;
		actual = rs.getInt(1);
		System.out.println("--There should be 2 attributes in the database.");
		assertEquals("There are no attributes in the database", expected,
				actual);
	}

	@Test
	public void testAddElements() throws Exception {
		System.out.println("Unit test for addElements");
		System.out
				.println("It should store 3 elements. 1 group element, 2 children.");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM ELEMENTS";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("--There should be no elements in the database.");
		assertEquals("There are elements in the database", expected, actual);
		element.addElements();
		element.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 3;
		actual = rs.getInt(1);
		System.out.println("--There should be 3 elements in the database.");
		assertEquals("There are no elements in the database", expected, actual);
	}

}
