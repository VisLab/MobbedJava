package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;

public class TestEntityQuery {
	private String[][] atts = {};
	private String[] cols = {};
	private String cursor = null;
	private String[] dcols = {};
	private double[][] dvals = {};
	private double limit = Double.POSITIVE_INFINITY;
	private String match = null;
	private double[][] ranges = {};
	private String regex = null;
	private String table = null;
	private String[][] tags = {};
	private String[][] vals = {};
	private static String database = "testentitydb";
	private static long[] datasetOids = {};
	private static UUID[] datasetUuids = {};
	private static UUID[] elementUuids = {};
	private static String host = "localhost";
	private static ManageDB md;
	private static String password = "admin";
	private static String sqlFile;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testDoubleSearch() throws Exception {
		System.out.println("Unit test for double search");
		table = "events";
		regex = "off";
		match = "exact";
		dcols = new String[] { "event_start_time" };
		dvals = new double[][] { { 1 } };
		ranges = new double[][] { { -2.220446049250313E-16,
				2.220446049250313E-16 } };
		String[][] rows = md.searchRows(table, limit, regex, match, tags, atts,
				cols, vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve one event that happens 1 second after the start of the dataset");
		assertEquals(1, rows.length);
		dvals = new double[][] { { 1, 2 } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve two events that happens 1 or 2 seconds after the start of the dataset\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testIntegerSearch() throws Exception {
		System.out.println("Unit test for integer search");
		table = "elements";
		regex = "off";
		match = "exact";
		cols = new String[] { "element_position" };
		vals = new String[][] { { "1" } };
		String[][] rows = md.searchRows(table, limit, regex, match, tags, atts,
				cols, vals, dcols, dvals, ranges, cursor);
		System.out.println("--It should retrieve one element with position 1");
		assertEquals(1, rows.length);
		vals = new String[][] { { "1", "2" } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve two elements by positions 1 and 2\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testOidSearch() throws Exception {
		System.out.println("Unit test for oid search");
		table = "datasets";
		regex = "off";
		match = "exact";
		cols = new String[] { "dataset_oid" };
		vals = new String[][] { { Long.toString(datasetOids[0]) } };
		String[][] rows = md.searchRows(table, limit, regex, match, tags, atts,
				cols, vals, dcols, dvals, ranges, cursor);
		System.out.println("--It should retrieve one dataset by oid");
		assertEquals(1, rows.length);
		vals = new String[][] { { Long.toString(datasetOids[0]),
				Long.toString(datasetOids[1]) } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out.println("--It should retrieve two datasets by oids\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testStringSearch() throws Exception {
		System.out
				.println("Unit test for string search without regular expressions");
		table = "contacts";
		regex = "off";
		match = "exact";
		cols = new String[] { "contact_last_name" };
		vals = new String[][] { { "Doe" } };
		String[][] rows = md.searchRows(table, limit, regex, match, tags, atts,
				cols, vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve two contacts with the last name Doe");
		assertEquals(2, rows.length);
		cols = new String[] { "contact_first_name", "contact_last_name" };
		vals = new String[][] { { "Tim" }, { "Smith" } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve one contact with first name Tim and last name Smith");
		assertEquals(1, rows.length);
		cols = new String[] { "contact_first_name" };
		vals = new String[][] { { "Tim", "Jane" } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve two contacts with first names Tim or Jane\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testStringSearchRegex() throws Exception {
		System.out
				.println("Unit test for string search with regular expressions");
		table = "contacts";
		regex = "on";
		match = "exact";
		cols = new String[] { "contact_first_name" };
		vals = new String[][] { { "^J" } };
		String[][] rows = md.searchRows(table, limit, regex, match, tags, atts,
				cols, vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve two contacts with first names that start with J");
		assertEquals(2, rows.length);
		cols = new String[] { "contact_first_name", "contact_last_name" };
		vals = new String[][] { { "^J" }, { "Doe" } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out
				.println("--It should retrieve two contacts with first names that start with J and last name Doe\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testUUIDSearch() throws Exception {
		System.out.println("Unit test for uuid search");
		table = "elements";
		regex = "off";
		match = "exact";
		cols = new String[] { "element_uuid" };
		vals = new String[][] { { elementUuids[0].toString() } };
		String[][] rows = md.searchRows(table, limit, regex, match, tags, atts,
				cols, vals, dcols, dvals, ranges, cursor);
		System.out.println("--It should retrieve one element by its UUID");
		assertEquals(1, rows.length);
		vals = new String[][] { { elementUuids[0].toString(),
				elementUuids[1].toString() } };
		rows = md.searchRows(table, limit, regex, match, tags, atts, cols,
				vals, dcols, dvals, ranges, cursor);
		System.out.println("--It should retrieve two elements by their UUID\n");
		assertEquals(2, rows.length);
	}

	public static void initializeValues() throws Exception {
		TestEntityQueryHelper.insertContacts(md.getConnection());
		datasetUuids = TestEntityQueryHelper.insertDatasets(md.getConnection());
		elementUuids = TestEntityQueryHelper.insertElements(md.getConnection());
		TestEntityQueryHelper.insertEvents(md.getConnection());
		datasetOids = TestEntityQueryHelper.insertDatasetOids(
				md.getConnection(), datasetUuids);
		md.commitTransaction();
	}

	@BeforeClass
	public static void setup() throws Exception {
		try {
			sqlFile = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
			md = new ManageDB(database, host, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(database, host, user, password, sqlFile,
					verbose);
			md = new ManageDB(database, host, user, password, verbose);
		} finally {
			initializeValues();
			md.setAutoCommit(true);
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.closeConnection();
		ManageDB.dropDatabase(database, host, user, password, verbose);
	}

}
