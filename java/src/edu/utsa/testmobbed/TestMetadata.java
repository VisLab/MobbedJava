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
import edu.utsa.testmobbed.helpers.Datasets;

/**
 * @author JCockfield
 * 
 */
public class TestMetadata {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "attributedb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = true;
	private static ManageDB md;
	private static Datasets dataset;
	private static Metadata metadata;

	@BeforeClass
	public static void setUp() throws Exception {
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
			String metadatafield = "metadata";
			metadata = new Metadata(md.getConnection());
			metadata.reset(datasetUuid, metadatafield);
		}

	}

	@AfterClass
	public static void closeConnection() throws Exception {
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
		String fieldName = "metadatafield";
		Double[] numAttrValues = { 0.123 };
		String[] attrValues = { "0.123" };
		metadata.addAttribute(fieldName, numAttrValues, attrValues);
		metadata.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 1;
		actual = rs.getInt(1);
		System.out.println("-- There should be 1 attributes in the database.");
		assertEquals("There are no attributes in the database", expected,
				actual);
	}

}