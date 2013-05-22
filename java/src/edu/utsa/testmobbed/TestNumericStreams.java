/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertTrue;

import java.net.URLDecoder;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.NumericStreams;

/**
 * Unit tests for NumericStreams class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestNumericStreams {
	private static String tablePath;
	private static String name = "numericstreamdb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;

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
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testStoreNumericStream() throws Exception {
		System.out.println("Unit test for storeNumericValue");
		System.out.println("It should store a numeric value data definition");
		String datadefValues[][] = { { null, "NUMERIC_VALUE", null, null,
				"NUMERIC_VALUE DATADEF" } };
		String[] doubleColumns = { "datadef_sampling_rate" };
		Double[][] doubleValues = { { 128.0 } };
		String[] datadefUuids = md.addRows("datadefs",
				md.getColumnNames("datadefs"), datadefValues, doubleColumns,
				doubleValues);
		double[][] expected = { { 1.0, 2.0 }, { 3.0, 4.0 }, { 5.0, 6.0 } };
		double[] times = { 0, .0078125 };
		NumericStreams ns = new NumericStreams(md.getConnection());
		ns.reset(datadefUuids[0]);
		ns.save(expected, times, 1);
		double[][] actual = flipArray(ns.retrieveByPosition(1, 3, 3));
		System.out
				.println("--It should retrieve a numeric streams data definition that is equal");
		assertTrue("Data definition returned in not equal",
				Arrays.deepEquals(expected, actual));

	}

	static double[][] flipArray(double[][] array) {
		double[][] temp = new double[array[0].length][array.length];
		for (int row = 0; row < temp.length; row++) {
			for (int column = 0; column < temp[0].length; column++)
				temp[row][column] = array[column][row];
		}
		return temp;
	}
}
