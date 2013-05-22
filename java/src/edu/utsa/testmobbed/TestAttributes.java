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
import edu.utsa.mobbed.Structures;

/**
 * Unit tests for Attributes class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestAttributes {
	private static String tablePath;
	private static String name = "attributedb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;
	private static String[] elementUuids;
	private static String[] datasetUuids;
	private static UUID structureUuid;
	private static Attributes attribute;

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
			datasetUuids = md.addRows("datasets",
					md.getColumnNames("datasets"), datasetValues, null, null);
			String[] elementLabels = { "channel 1", "channel 2" };
			String[] elementDescriptions = { "EEG channel: 1", "EEG channel: 2" };
			long[] elementPositions = { 1, 2 };
			Elements element = new Elements(md.getConnection());
			element.reset("EEG", datasetUuids[0], "chanlocs", "EEG CAP",
					elementLabels, elementDescriptions, elementPositions);
			elementUuids = element.addElements();
			UUID parentStructUuid = UUID.randomUUID();
			Structures structure = new Structures(md.getConnection());
			structure.reset(parentStructUuid, "EEG",
					UUID.fromString(ManageDB.noParentUuid));
			structure.save();
			UUID elementStructUuid = UUID.randomUUID();
			structure.reset(elementStructUuid, "element", parentStructUuid);
			structure.save();
			structureUuid = UUID.randomUUID();
			structure.reset(structureUuid, "X", elementStructUuid);
			structure.save();
			attribute = new Attributes(md.getConnection());

		}

	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

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
				"elements", UUID.fromString(datasetUuids[0]), "datasets",
				structureUuid, 0.123, "0.123");
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
}
