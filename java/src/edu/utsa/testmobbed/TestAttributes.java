/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertEquals;

import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.Attributes;
import edu.utsa.mobbed.Elements;
import edu.utsa.mobbed.ManageDB;

/**
 * Unit tests for Attributes class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestAttributes {
	private static Attributes attribute;
	private static String[] datasetUuids;
	private static String[] elementUuids;
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "attributedb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testAddToBatch() throws Exception {
		System.out.println("Unit test for addToBatch");
		System.out
				.println("It should add a attribute to the batch and save it to the database");
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
		attribute.reset(UUID.randomUUID(), UUID.fromString(elementUuids[0]),
				"elements", UUID.fromString(datasetUuids[0]), "/chanlocs",
				0.123, "0.123");
		attribute.addToBatch();
		attribute.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 1;
		actual = rs.getInt(1);
		System.out.println("--There should be 1 attribute in the database.");
		assertEquals("There are no attributes in the database", expected,
				actual);

	}

	@BeforeClass
	public static void setup() throws Exception {
		try {
			ManageDB.dropDatabase(name, hostname, user, password, verbose);
		} catch (Exception e) {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
			String datasetValues[][] = { { null, null, null, "ELEMENT_DATASET",
					null, null, null, "ELEMENT DATASET", null, null, null } };
			datasetUuids = md.addRows("datasets",
					md.getColumnNames("datasets"), datasetValues, null, null);
			String[] elementLabels = { "channel 1", "channel 2" };
			String[] elementDescriptions = { "EEG channel: 1", "EEG channel: 2" };
			long[] elementPositions = { 1, 2 };
			Elements element = new Elements(md.getConnection());
			element.reset(datasetUuids[0], "EEG CAP", elementLabels,
					elementDescriptions, elementPositions);
			elementUuids = element.addElements();
			attribute = new Attributes(md.getConnection());
		}

	}

	@AfterClass
	public static void teardown() throws Exception {
		md.closeConnection();
		ManageDB.dropDatabase(name, hostname, user, password, verbose);
	}
}
