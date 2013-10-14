/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertEquals;

import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.Metadata;

/**
 * Unit tests for Metadata class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestMetadata {
	private static String hostname = "localhost";
	private static ManageDB md;
	private static Metadata metadata;
	private static String name = "metadatadb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testAddAttribute() throws Exception {
		System.out.println("Unit test for addAtrribute");
		System.out.println("It should store a metadata attribute");
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
		Double[] numAttrValues = { 0.123 };
		String[] attrValues = { "0.123" };
		metadata.addAttribute(fieldName, numAttrValues, attrValues);
		metadata.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 1;
		actual = rs.getInt(1);
		System.out.println("--There should be 1 attributes in the database.");
		assertEquals("There are no attributes in the database", expected,
				actual);
	}

	@AfterClass
	public static void closeConnection() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@BeforeClass
	public static void setUp() throws Exception {
		try {
			ManageDB.deleteDatabase(name, hostname, user, password, verbose);
		} catch (Exception e) {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
			String datasetValues[][] = { { null, null, null,
					"METADATA_DATASET", null, null, null, "METADATA_DATASET",
					null, null, null } };
			String[] datasetUuids = md.addRows("datasets",
					md.getColumnNames("datasets"), datasetValues, null, null);
			metadata = new Metadata(md.getConnection());
			metadata.reset(datasetUuids[0]);
		}

	}
}