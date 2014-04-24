package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;

public class TestGroupQuery {
	private String[][] attributes = {};
	private String[] columns = {};
	private String cursorName = null;
	private String[] doubleColumns = {};
	private double[][] doubleValues = {};
	private double limit = Double.POSITIVE_INFINITY;
	private double[][] range = {};
	private String regex = null;
	private String table = null;
	private String tagMatch = null;
	private String[][] tags = {};
	private String[][] values = {};
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "testgroupdb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testAttributes() throws Exception {
		System.out
				.println("Unit test for attributes without regular expressions");
		table = "events";
		regex = "off";
		tagMatch = "exact";
		attributes = new String[][] { { "1" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("----It should retrieve one event that has an attribute value 1\n");
		assertEquals(1, rows.length);
	}

	@Test
	public void testAttributesRegex() throws Exception {
		System.out.println("Unit test for attributes with regular expressions");
		table = "events";
		regex = "on";
		tagMatch = "exact";
		attributes = new String[][] { { "^Attribute" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve two events that have an attribute value that starts with Attribute\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testExactMatch() throws Exception {
		System.out.println("Unit test for exact tag match");
		table = "events";
		regex = "off";
		tagMatch = "exact";
		tags = new String[][] { { "/Context/Indoors" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve one event that has the tag /Context/Indoors\n");
		assertEquals(1, rows.length);
	}

	@Test
	public void testExactMatchAND() throws Exception {
		System.out.println("Unit test for exact tag match with AND condition");
		table = "events";
		regex = "off";
		tagMatch = "exact";
		tags = new String[][] { { "/Context/Running" }, { "/State/Awake" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve one event that has the tags /Context/Running and /State/Awake\n");
		assertEquals(1, rows.length);
	}

	@Test
	public void testExactMatchOR() throws Exception {
		System.out.println("Unit test for exact tag match with OR condition");
		table = "events";
		regex = "off";
		tagMatch = "exact";
		tags = new String[][] { { "/Context/Running", "/Context/Indoors" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve two events that have the tags /Context/Running or /State/Awake\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testPrefixMatch() throws Exception {
		System.out.println("Unit test for prefix tag match");
		table = "events";
		regex = "off";
		tagMatch = "prefix";
		tags = new String[][] { { "/Context/" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve two events that have tags that start with /Context/\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testPrefixMatchAND() throws Exception {
		System.out.println("Unit test for exact tag match with AND condition");
		table = "events";
		regex = "off";
		tagMatch = "prefix";
		tags = new String[][] { { "/Context/" }, { "/State/" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve one event that has tags that start with /Context/ and /State/\n");
		assertEquals(1, rows.length);
	}

	@Test
	public void testPrefixMatchOR() throws Exception {
		System.out.println("Unit test for exact tag match with OR condition");
		table = "events";
		regex = "off";
		tagMatch = "prefix";
		tags = new String[][] { { "/Context/", "/State/" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve two events that have tags that start with /Context/ or /State/\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testWordMatch() throws Exception {
		System.out.println("Unit test for word tag match");
		table = "events";
		regex = "off";
		tagMatch = "word";
		tags = new String[][] { { "Context" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve two events that have a tag that contains the word Context\n");
		assertEquals(2, rows.length);
	}

	@Test
	public void testWordMatchAND() throws Exception {
		System.out.println("Unit test for exact tag match with AND condition");
		table = "events";
		regex = "off";
		tagMatch = "word";
		tags = new String[][] { { "Context" }, { "Awake" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve one event that has tags that contains the words Context and Awake\n");
		assertEquals(1, rows.length);
	}

	@Test
	public void testWordMatchOR() throws Exception {
		System.out.println("Unit test for exact tag match with OR condition");
		table = "events";
		regex = "off";
		tagMatch = "word";
		String[][] tags = new String[][] { { "Running", "Indoors" } };
		String[][] rows = md.searchRows(table, limit, regex, tagMatch, tags,
				attributes, columns, values, doubleColumns, doubleValues,
				range, cursorName);
		System.out
				.println("--It should retrieve two events that have tags that contains the words Running or Indoors\n");
		assertEquals(2, rows.length);
	}

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
			TestGroupQueryHelper.populateDatabase(md.getConnection());
			md.commitTransaction();
		}
		md.setAutoCommit(true);
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.closeConnection();
		ManageDB.dropDatabase(name, hostname, user, password, verbose);
	}

}
