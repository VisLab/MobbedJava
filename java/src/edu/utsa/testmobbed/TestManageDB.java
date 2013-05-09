/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.Elements;
import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.MobbedException;

/**
 * @author JCockfield
 * 
 */
public class TestManageDB {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "testmanagedb";
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
		}
		md.setAutoCommit(true);
		String[][] contactValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows("contacts", md.getColumnNames("contacts"), contactValues,
				null, null);
		String datasetValues[][] = { { null, null, null, "MANAGEDB_DATASET",
				null, null, null, "MANAGEDB_DATASET", null, null, null } };
		String[] datasetUuids = md.addRows("datasets",
				md.getColumnNames("datasets"), datasetValues, null, null);
		String[] elementLabels = { "channel 1" };
		String[] elementDescriptions = { "EEG channel: 1" };
		long[] elementPositions = { 1 };
		Elements element = new Elements(md.getConnection());
		element.reset(datasetUuids[0], "chanlocs", "EEG CAP", elementLabels,
				elementDescriptions, elementPositions);
		String[] elementUuids = element.addElements();
		String[][] attributeValues = {
				{ null, elementUuids[0], "elements", datasetUuids[0],
						"datasets", null, null, "Alpha" },
				{ null, elementUuids[0], "elements", datasetUuids[0],
						"datasets", null, null, "Beta" },
				{ null, elementUuids[0], "elements", datasetUuids[0],
						"datasets", null, null, "Omega" } };
		md.addRows("attributes", md.getColumnNames("attributes"),
				attributeValues, null, null);
		String[][] tagValues = { { "EyeTrack", datasetUuids[0], "datasets" },
				{ "VisualTarget", datasetUuids[0], "datasets" } };
		md.addRows("tags", md.getColumnNames("tags"), tagValues, null, null);
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);

	}

	@Test
	public void testGetColumnNames() throws Exception {
		System.out.println("Unit test for getColumnNames:");
		System.out
				.println("It should get the column names of the datasets table");
		String tableName = "datasets";
		String[] actual = md.getColumnNames(tableName);
		assertNotNull("Columns names are null", actual);
		String sql = "SELECT column_name from information_schema.columns where table_schema = 'public' AND table_name = ?";
		PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
		pstmt.setString(1, tableName);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<String> columns = new ArrayList<String>();
		while (rs.next())
			columns.add(rs.getString(1));
		String[] expected = columns.toArray(new String[columns.size()]);
		System.out
				.println("--It should return the expected column names for the given table datasets");
		assertArrayEquals(
				"Column names are not equal to the expected column names",
				expected, actual);
	}

	@Test
	public void testGetDefaultValue() throws Exception {
		System.out.println("Unit test for getDefaultValue:");
		System.out
				.println("It should get the default value of the dataset_namespace column");
		String tableName = "datasets";
		String columnName = "dataset_namespace";
		String actual = md.getDefaultValue(columnName);
		assertNotNull("Default value is null", actual);
		String sql = "SELECT column_name, column_default from information_schema.columns where table_schema = 'public' AND table_name = ?";
		PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
		pstmt.setString(1, tableName);
		ResultSet rs = pstmt.executeQuery();
		String defaultValue = null;
		HashMap<String, String> defaults = new HashMap<String, String>();
		while (rs.next()) {
			defaultValue = rs.getString(2);
			if (defaultValue != null)
				defaultValue = defaultValue.split(":")[0].replaceAll("'", "");
			defaults.put(rs.getString(1), defaultValue);
		}
		String expected = defaults.get(columnName);
		System.out
				.println("--It should return the expected default value for the given column dataset_namespace");
		assertEquals(
				"Default column name is not equal to the expected default column name",
				expected, actual);
	}

	@Test
	public void testGetDoubleColumns() throws Exception {
		System.out.println("Unit test for getDoubleColumns:");
		System.out
				.println("It should get the double columns of the attributes table");
		String tableName = "attributes";
		String columnQuery = "SELECT column_name from information_schema.columns where table_name = ? AND table_schema = 'public' AND data_type = 'double precision' ";
		PreparedStatement pstmt = md.getConnection().prepareStatement(
				columnQuery);
		pstmt.setString(1, tableName);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<String> al = new ArrayList<String>();
		while (rs.next())
			al.add(rs.getString(1));
		String[] expected = al.toArray(new String[al.size()]);
		String[] actual = md.getDoubleColumns(tableName);
		System.out
				.println("--It should return the expected double columns for the given table attributes");
		assertArrayEquals(
				"Double columns are not equal to expected double columns",
				expected, actual);
	}

	@Test
	public void testGetColumnType() throws Exception {
		System.out.println("Unit test for getColumnType:");
		System.out
				.println("It should get the type of the dataset_namespace column");
		String tableName = "datasets";
		String columnName = "dataset_namespace";
		String actual = md.getColumnType(columnName);
		assertNotNull(actual);
		String sql = "SELECT column_name, data_type from information_schema.columns where table_schema = 'public' AND table_name = ?";
		PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
		pstmt.setString(1, tableName);
		ResultSet rs = pstmt.executeQuery();
		HashMap<String, String> types = new HashMap<String, String>();
		while (rs.next())
			types.put(rs.getString(1), rs.getString(2));
		String expected = types.get(columnName);
		System.out
				.println("--It should return the expected column type for the given column dataset_namespace");
		assertEquals("Column type is not equal to expected column type",
				expected, actual);
	}

	@Test
	public void testGetKeys() throws Exception {
		System.out.println("Unit test for getKeys:");
		System.out.println("It should get the keys of the datasets table");
		String tableName = "datasets";
		String[] actual = md.getKeys(tableName);
		assertNotNull(actual);
		String sql = "SELECT pg_attribute.attname FROM pg_index, pg_class, pg_attribute"
				+ " WHERE pg_class.oid = ?"
				+ "::regclass AND"
				+ " indrelid = pg_class.oid AND"
				+ " pg_attribute.attrelid = pg_class.oid AND"
				+ " pg_attribute.attnum = any(pg_index.indkey)"
				+ " AND indisprimary";
		PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
		pstmt.setString(1, tableName);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<String> keys = new ArrayList<String>();
		while (rs.next())
			keys.add(rs.getString(1));
		String[] expected = keys.toArray(new String[keys.size()]);
		System.out
				.println("--It should return the expected keys for the given table datasets");
		assertArrayEquals("Keys are not equal to the expected keys", expected,
				actual);
	}

	@Test
	public void testAddRowsCompositeKey() throws Exception {
		System.out
				.println("Unit test for addRows with table with a composite primary key:");
		System.out.println("It should store a tag into the database");
		String tableName = "tags";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		String[] actual = md.addRows(tableName, columnNames, columnValues,
				null, null);
		assertNotNull(actual);
		String[] expected = { columnValues[0][0] + "," + columnValues[0][1] };
		System.out
				.println("--It should return a comma separated composite primary key for the given table tags");
		assertArrayEquals(
				"The keys returned are not equal to the expected keys",
				expected, actual);
	}

	@Test
	public void testgetTables() throws Exception {
		System.out.println("Unit test for getTables:");
		System.out.println("It should retrieve all of the database tables");
		String[] actual = md.getTables();
		java.util.Arrays.sort(actual);
		assertNotNull(actual);
		Statement stmt = md.getConnection().createStatement();
		String tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
		ResultSet rs = stmt.executeQuery(tableQuery);
		ArrayList<String> tables = new ArrayList<String>();
		while (rs.next())
			tables.add(rs.getString(1));
		String[] expected = tables.toArray(new String[tables.size()]);
		System.out
				.println("--It should return all of the tables in the database");
		assertArrayEquals("Tables are not equal to expected tables", expected,
				actual);
	}

	@Test
	public void testgetColumnTypes() throws Exception {
		System.out.println("Unit test for getColumnTypes:");
		System.out
				.println("It should return all of the column types in the datasets table");
		String tableName = "datasets";
		String[] actual = md.getColumnTypes(tableName);
		assertNotNull(actual);
		String sql = "SELECT data_type from information_schema.columns where table_schema = 'public' AND table_name = ?";
		PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
		pstmt.setString(1, tableName);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<String> types = new ArrayList<String>();
		while (rs.next())
			types.add(rs.getString(1));
		String[] expected = types.toArray(new String[types.size()]);
		System.out
				.println("--It should return the expected column types for the given table datasets");
		assertArrayEquals("Tables are not equal to expected tables", expected,
				actual);
	}

	@Test
	public void testRetrieveRowsLimit() throws Exception {
		System.out.println("Unit test for retrieveRows with a limit:");
		System.out
				.println("It should retrieve at most 1 contact from the database");
		String[][] rows = md.retrieveRows("contacts", 1, "off", null, null,
				null, null);
		assertNotNull(rows);
		int actual = rows.length;
		int expected = 1;
		System.out
				.println("--It should return at most the limit which is 1 of rows in the contacts table");
		assertEquals(
				"The number of rows returned is not equal to the exepect rows",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsNoLimit() throws Exception {
		System.out.println("Unit test for retrieveRows with a no limit:");
		System.out
				.println("It should retrieve all the contacts in the database");
		String[][] rows = md.retrieveRows("contacts", Double.POSITIVE_INFINITY,
				"off", null, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM CONTACTS";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return all of the rows in the contacts table");
		assertEquals(
				"The number of rows returned is not equal to the exepect rows",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureNoRegExp() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure search specifications:");
		System.out
				.println("It should retrieve all contacts with first name john");
		String[] columnNames = { "contact_first_name" };
		String[][] columnValues = { { "john" } };
		String[][] rows = md.retrieveRows("contacts", Double.POSITIVE_INFINITY,
				"off", null, null, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM CONTACTS WHERE UPPER(contact_first_name) = 'JOHN'";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the contacts table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);

	}

	@Test
	public void testRetrieveRowsStructureRegExp() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure search specifications passed in as regular expressions:");
		System.out
				.println("It should retrieve all contacts whose name begin with jo");
		String[] columnNames = { "contact_first_name" };
		String[][] columnValues = { { "jo*" } };
		String[][] rows = md.retrieveRows("contacts", Double.POSITIVE_INFINITY,
				"on", null, null, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM CONTACTS WHERE contact_first_name ~* 'Jo*'";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the contacts table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureMultipleValues() throws Exception {
		System.out
				.println("Unit test for retrieveRows with multiple structure search specifications for a given column:");
		System.out
				.println("It should retrieve all contacts whose first name is either John or Jane and last name is Doe");
		String[] columnNames = { "contact_first_name", "contact_last_name" };
		String[][] columnValues = { { "John", "Jane" }, { "Doe" } };
		String[][] rows = md.retrieveRows("contacts", Double.POSITIVE_INFINITY,
				"off", null, null, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM CONTACTS"
				+ " WHERE UPPER(contact_first_name) IN('JOHN', 'JANE') AND UPPER(contact_last_name) IN('DOE')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the contacts table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureMultipleValuesRegExp()
			throws Exception {
		System.out
				.println("Unit test for retrieveRows with multiple structure search specifications for a given column that is passed in as regular expressions:");
		System.out
				.println("It should retrieve all contacts whose first name begin with either Jo or Ja and last name is Doe");
		String[] columnNames = { "contact_first_name", "contact_last_name" };
		String[][] columnValues = { { "Jo*", "Ja*" }, { "Doe" } };
		String[][] rows = md.retrieveRows("contacts", Double.POSITIVE_INFINITY,
				"on", null, null, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM CONTACTS"
				+ " WHERE contact_first_name ~* 'JO*|JA*' AND contact_last_name ~* 'DOE'";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the contacts table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsORCondition() throws Exception {
		System.out
				.println("Unit test for retrieveRows with tags using the OR condition:");
		System.out
				.println("It should retrieve all datasets that have the tag EyeTrack or VisualTarget");
		String[][] tagValues = { { "EyeTrack", "VisualTarget" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"off", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsRegExp() throws Exception {
		System.out
				.println("Unit test for retrieveRows with tags passed in as regular expressions:");
		System.out
				.println("It should retrieve all datasets that have a tag that begins with Eye");
		String[][] tagValues = { { "Eye*" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"on", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'Eye*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsANDORCondition() throws Exception {
		System.out
				.println("Unit test for retrieveRows with tags using the AND and OR condition:");
		System.out
				.println("It should retrieve all datasets that have the tag EyeTrack or VisualTarget and AudioLeft");
		String[][] tagValues = { { "EyeTrack", "VisualTarget" },
				{ "AudioLeft" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"off", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET') INTERSECT SELECT TAG_ENTITY_UUID FROM TAGS WHERE UPPER(TAG_NAME) IN('AUDIOLEFT'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsAttributesRegExp() throws Exception {
		System.out
				.println("Unit test for retrieveRows with attributes passed in as regular expressions:");
		String[][] attributeValues = { { "A*", "B*" } };
		System.out
				.println("It should retrieve all datasets that have attributes that begin with A and B");
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"on", null, attributeValues, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsAttributesANDORCondition() throws Exception {
		System.out
				.println("Unit test for retrieveRows with attributes using the AND and OR condition:");
		System.out
				.println("It should retrieve all datasets that have atrritubes ALPHA or BETA and Omega");
		String[][] attributeValues = { { "ALPHA", "BETA" }, { "Omega" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"off", null, attributeValues, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN ('ALPHA', 'BETA') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN ('OMEGA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureAttributes() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure and attribute search specifications:");
		System.out
				.println("It should retrieve all datasets that have the name MANAGEDB_DATASET and atrritubes ALPHA or BETA and Omega");
		String[] columnNames = { "dataset_name" };
		String[][] columnValues = { { "MANAGEDB_DATASET" } };
		String[][] attributeValues = { { "Alpha", "Beta" }, { "Omega" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"off", null, attributeValues, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT dataset_uuid from DATASETS WHERE UPPER(DATASET_NAME) IN('MANAGEDB_DATASET') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('ALPHA','BETA') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('OMEGA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureAttributesRegExp() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure and attribute search specifications passed in as regular expressions:");
		System.out
				.println("It should retrieve all datasets that have the name that begin with MANAGEDB and atrritubes that begin with A or B and O");
		String[] columnNames = { "dataset_name" };
		String[][] columnValues = { { "MANAGEDB*" } };
		String[][] attributeValues = { { "A*", "B*" }, { "O*" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"on", null, attributeValues, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT dataset_uuid from DATASETS WHERE DATASET_NAME ~* 'MANAGEDB*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'O*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTags() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure and tag search specifications:");
		System.out
				.println("It should retrieve all datasets that have the name MANAGEDB_DATASET and tags EyeTrack or VisualTarget");
		String[] columnNames = { "dataset_name" };
		String[][] columnValues = { { "MANAGEDB_DATASET" } };
		String[][] tagValues = { { "EyeTrack", "VisualTarget" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"off", tagValues, null, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE UPPER(DATASET_NAME) IN('MANAGEDB_DATASET') INTERSECT SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsRegExp() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure and tag search specifications passed in as regular expressions:");
		System.out
				.println("It should retrieve all datasets that have the name that begin with MANAGEDB and tags that begin with Eye or Visual");
		String[] columnNames = { "dataset_name" };
		String[][] columnValues = { { "MANAGEDB*" } };
		String[][] tagValues = { { "Eye*", "Visual*" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"on", tagValues, null, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE DATASET_NAME ~* 'MANAGEDB*' INTERSECT SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'Eye*|Visual*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsAttributes() throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure, tag, and attribute search specifications:");
		System.out
				.println("It should retrieve all datasets that have the name MANAGEDB_DATASET and tags EyeTrack or VisualTarget and attributes ALPHA or BETA and OMEGA");
		String[] columnNames = { "dataset_name" };
		String[][] columnValues = { { "MANAGEDB_DATASET" } };
		String[][] tagValues = { { "EyeTrack", "VisualTarget" } };
		String[][] attributeValues = { { "Alpha", "Beta" }, { "Omega" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"off", tagValues, attributeValues, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS "
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE UPPER(DATASET_NAME) IN('MANAGEDB_DATASET') INTERSECT SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET')INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('ALPHA','BETA') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('OMEGA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsAttributesRegExp()
			throws Exception {
		System.out
				.println("Unit test for retrieveRows with structure, tag, and attribute search specifications passed in as regular expressions:");
		System.out
				.println("It should retrieve all datasets that have the name that begin with MANAGEDB and tags that begin with Eye or Visual and attributes that begin with A or B and O");
		String[] columnNames = { "dataset_name" };
		String[][] columnValues = { { "MANAGEDB*" } };
		String[][] tagValues = { { "Eye*", "Visual*" } };
		String[][] attributeValues = { { "A*", "B*" }, { "O*" } };
		String[][] rows = md.retrieveRows("datasets", Double.POSITIVE_INFINITY,
				"on", tagValues, attributeValues, columnNames, columnValues);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM DATASETS"
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE DATASET_NAME ~* 'MANAGEDB*' INTERSECT SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'Eye*|Visual*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'O*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		System.out
				.println("--It should return the expected rows from the datasets table given the search criteria");
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidTableName() throws Exception {
		System.out.println("Unit test for addRows with invalid table:");
		System.out
				.println("It should throw an exception when passing in the table invalid_table");
		String tableName = "invalid_table";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		System.out
				.println("--It should throw an exception when specifying an invalid table");
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidColumnNames() throws Exception {
		System.out.println("Unit test for addRows with invalid columns names:");
		System.out
				.println("It should throw an exception when passing in the invalid column names invalid_column1 and invalid_column2 into the tags table");
		String tableName = "tags";
		String[] columnNames = { "invalid_column1", "invalid_column2",
				"invalid_column3" };
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		System.out
				.println("--It should throw an exception when specifying an invalid column name");
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidColumnValues() throws Exception {
		System.out
				.println("Unit test for addRows with invalid columns values:");
		System.out
				.println("It should throw an exception when passing in the invalid column values invalid_value1, invalid_value2, and invalid_value3 into the datasets table");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "invalid_value1", "invalid_value2",
				"invalid_value3" } };
		System.out
				.println("--It should throw an exception when specifying an invalid column value");
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

}
