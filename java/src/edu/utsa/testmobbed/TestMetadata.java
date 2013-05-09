/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.Metadata;

/**
 * @author JCockfield
 * 
 */
public class TestMetadata {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "metadatadb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;
	private static Metadata metadata;

	@BeforeClass
	public static void setUp() throws Exception {
		try {
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
			metadata = new Metadata(md.getConnection());
			metadata.reset(datasetUuids[0], "metadata");
		}

	}

	@AfterClass
	public static void closeConnection() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testAddAttribute() throws Exception {
		System.out.println("Unit test for addAtrribute");
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
		String fieldName = "metadatafield";
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
}