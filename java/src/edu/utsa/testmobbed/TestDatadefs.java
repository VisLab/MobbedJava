/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.Datadefs;
import edu.utsa.mobbed.ManageDB;

/**
 * Unit tests for Datadefs class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestDatadefs {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "datadefdb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;

	@BeforeClass
	public static void setup() throws Exception {
		try {
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testStoreNumericValue() throws Exception {
		System.out.println("Unit test for storeNumericValue");
		System.out.println("It should store a numeric value data definition");
		String datadefValues[][] = { { null, "NUMERIC_VALUE", null, null,
				"NUMERIC_VALUE DATADEF" } };
		String[] doubleColumns = { "datadef_sampling_rate" };
		Double[][] doubleValues = { { null } };
		String[] datadefUuids = md.addRows("datadefs",
				md.getColumnNames("datadefs"), datadefValues, doubleColumns,
				doubleValues);
		Object[] expected = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0 };
		Datadefs.storeNumericValue(md.getConnection(), datadefUuids[0],
				expected);
		Object actual = Datadefs.retrieveNumericValue(md.getConnection(),
				datadefUuids[0]);
		System.out
				.println("--It should retrieve a numeric value data definition that is equal");
		assertTrue("Data definition returned in not equal",
				Arrays.equals(expected, (Object[]) actual));
	}

	@Test
	public void testStoreXMLValue() throws Exception {
		System.out.println("Unit test for storeXMLValue");
		System.out.println("It should store a xml value data definition");
		String datadefValues[][] = { { null, "XML_VALUE", null, null,
				"NUMERIC_VALUE DATADEF" } };
		String[] doubleColumns = { "datadef_sampling_rate" };
		Double[][] doubleValues = { { null } };
		String[] datadefUuids = md.addRows("datadefs",
				md.getColumnNames("datadefs"), datadefValues, doubleColumns,
				doubleValues);
		String expected = "<xml> <tag1> valid xml </tag1> </xml>";
		Datadefs.storeXMLValue(md.getConnection(), datadefUuids[0], expected);
		String actual = Datadefs.retrieveXMLValue(md.getConnection(),
				datadefUuids[0]);
		System.out
				.println("--It should retrieve a xml value data definition that is equal");
		assertEquals("Data definition returned in not equal", expected, actual);
	}
}
