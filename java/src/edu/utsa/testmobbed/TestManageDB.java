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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.MobbedException;
import edu.utsa.testmobbed.helpers.Datasets;

/**
 * @author JCockfield
 * 
 */
public class TestManageDB {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose = false;
	private ManageDB md;

	@Before
	public void setupBeforeTest() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password);
		}
	}

	@Test
	public void testConstructor() throws Exception {
		System.out
				.println("TEST: testing establishConnection() method. There should be a connection established when a ManageDatabase object is created");
		assertNotNull("Connection is not established", md.getConnection());
		System.out
				.println("TEST: testing the default auto-commit mode. The autoCommit mode should be set to false when a ManageDatabase object is created");
		assertFalse("The auto-commit mode is true", md.getConnection()
				.getAutoCommit());
	}

	@Test
	public void testCreateDatabase() throws Exception {
		System.out
				.println("TEST: testing createDatabase() method. There should be a successful connection established to a new database");
		String name = "testdb2";
		ManageDB.createDatabase(name, hostname, user, password, tablePath,
				verbose);
		ManageDB md2 = new ManageDB(name, hostname, user, password);
		assertNotNull("Connection to the new database is not established",
				md2.getConnection());
		md2.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testDeleteDatabase() throws Exception {
		System.out
				.println("TEST: testing deleteDatabase() method. There should be a database deleted");
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test(expected = Exception.class)
	public void testDatabaseAlredyExists() throws Exception {
		System.out
				.println("TEST: creating a new database that already exist. There should be an exception thrown when trying to create a database that already exists");
		ManageDB.createDatabase(name, hostname, user, password, tablePath,
				verbose);
	}

	@Test(expected = Exception.class)
	public void testDatabaseDoesNotExists() throws Exception {
		System.out
				.println("TEST: deleting a database that does not exist. There should be an exception thrown when trying to delete a database that does not exists");
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testGetColumnNames() throws Exception {
		System.out
				.println("TEST: testing getColumnName() method. The column names should be retrieved when a given table name is specified");
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
		assertArrayEquals(
				"Column names are not equal to the expected column names",
				expected, actual);
	}

	@Test
	public void testGetDefaultValue() throws Exception {
		System.out
				.println("TEST: testing getDefaultValue() method. The default value should be retrieved when a given column name is specified");
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
		assertEquals(
				"Default column name is not equal to the expected default column name",
				expected, actual);
	}

	@Test
	public void testGetDoubleColumns() throws Exception {
		System.out
				.println("TEST: testing getDoubleColumns() method. The columns that are type double should be returned.");
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
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testGetColumnType() throws Exception {
		System.out
				.println("TEST: testing getColumnType() method. The column type should be retrieved when a given column name is specified");
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
		assertEquals("Column type is not equal to expected column type",
				expected, actual);
	}

	@Test
	public void testGetKeys() throws Exception {
		System.out
				.println("TEST: testing getKeys() method. The keys should be retrieved when a given table name is specified");
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
		assertArrayEquals("Keys are not equal to the expected keys", expected,
				actual);
	}

	@Test
	public void testAddRowsDoubleValues() throws Exception {
		String tableName = "attributes";
		String[] columnNames = md.getColumnNames(tableName);
		String[] doubleColumnName = md.getDoubleColumns(tableName);
		String[][] columnValues = {
				{ null, UUID.randomUUID().toString(),
						UUID.randomUUID().toString(),
						UUID.randomUUID().toString(), "1", "1.25" },
				{ null, UUID.randomUUID().toString(),
						UUID.randomUUID().toString(),
						UUID.randomUUID().toString(), "2", "2.85" },
				{ null, UUID.randomUUID().toString(),
						UUID.randomUUID().toString(),
						UUID.randomUUID().toString(), "3", "5.67" } };
		Double[][] doubleColumnValues = { { 1.25 }, { 2.85 }, { 5.67 } };
		md.addRows(tableName, columnNames, columnValues, doubleColumnName,
				doubleColumnValues);
	}

	@Test
	public void testAddRowsCompositeKey() throws Exception {
		System.out
				.println("TEST: testing addRows() method. The addRows method should return a comma separated key when the key is composite");
		String tableName = "tags";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		String[] actual = md.addRows(tableName, columnNames, columnValues,
				null, null);
		assertNotNull(actual);
		String[] expected = { columnValues[0][0] + "," + columnValues[0][1] };
		assertArrayEquals(
				"The keys returned are not equal to the expected keys",
				expected, actual);
		md.commit();
	}

	@Test
	public void testAddRowsSingleKey() throws Exception {
		System.out
				.println("TEST: testing addRows() method. The addRows method should return a single key if not composite");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { null, "John", "Doe", "J",
				"123 Doe Drive", "456 Doe Drive", "Doeville", "Florida", "USA",
				"45353", "124-412-4574", "jdoe@email.com" } };
		String[] actual = md.addRows(tableName, columnNames, columnValues,
				null, null);
		assertNotNull(actual);
		String[] expected = { columnValues[0][0] };
		assertArrayEquals(
				"The keys returned are not equal to the expected keys",
				expected, actual);
		md.commit();
	}

	@Test
	public void testAddRowsMultipleRows() throws Exception {
		System.out
				.println("TEST: testing addRows() method. The addRows method should store multiple rows in the database and return multiple keys");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		String[] actual = md.addRows(tableName, columnNames, columnValues,
				null, null);
		assertNotNull(actual);
		String[] expected = { columnValues[0][0], columnValues[1][0] };
		assertArrayEquals(
				"The keys returned are not equal to the expected keys",
				expected, actual);
		md.commit();
	}

	@Test
	public void testgetTables() throws Exception {
		System.out
				.println("TEST: testing getTables() method. The getTables method should return all the tables in the database");
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
		assertArrayEquals("Tables are not equal to expected tables", expected,
				actual);
	}

	@Test
	public void testgetColumnTypes() throws Exception {
		System.out
				.println("TEST: testing getColumnTypes() method. The getColumnTypes method should return all the column types from a table");
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
		assertArrayEquals("Tables are not equal to expected tables", expected,
				actual);
	}

	@Test
	public void testRetrieveRowsNoCriteriaLimit() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve at most the number of rows specified by the limit");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		md.commit();
		String[][] rows = md.retrieveRows(tableName, 1, "off", null, null,
				null, null);
		assertNotNull(rows);
		int actual = rows.length;
		int expected = 1;
		assertEquals(
				"The number of rows returned is not equal to the exepect rows",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsNoCriteriaNoLimit() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows when -1 is specified as the limit ");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		md.commit();
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", null, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM " + tableName;
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"The number of rows returned is not equal to the exepect rows",
				expected, actual);

	}

	@Test
	public void testRetrieveRowsStructureSingleColumnNoRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure fields passed in");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		md.commit();
		String[] columnNames2 = { "contact_first_name" };
		String[][] columnValues2 = { { "john" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", null, null, columnNames2, columnValues2);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM " + tableName
				+ " WHERE UPPER(contact_first_name) = 'JOHN'";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);

	}

	@Test
	public void testRetrieveRowsStructureSingleColumnRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure fields passed in");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		md.commit();
		String[] columnNames2 = { "contact_first_name" };
		String[][] columnValues2 = { { "jo*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", null, null, columnNames2, columnValues2);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM " + tableName
				+ " WHERE contact_first_name ~* 'Jo*'";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureMultipleColumnsNoRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure fields passed in");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		md.commit();
		String[] columnNames2 = { "contact_first_name", "contact_last_name" };
		String[][] columnValues2 = { { "John", "Jane" }, { "Doe" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", null, null, columnNames2, columnValues2);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE UPPER(contact_first_name) IN('JOHN', 'JANE') AND UPPER(contact_last_name) IN('DOE')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureMultipleColumnsRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure fields passed in");
		String tableName = "contacts";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		md.commit();
		String[] columnNames2 = { "contact_first_name", "contact_last_name" };
		String[][] columnValues2 = { { "JO*", "JA*" }, { "Doe" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", null, null, columnNames2, columnValues2);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE contact_first_name ~* 'JO*|JA*' AND contact_last_name ~* 'DOE'";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsSingleGroupNoRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the tags fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] tagValues = { { "EyeTrack", "VisualTarget" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsSingleGroupRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the tags fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] tagValues = { { "Eye*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'Eye*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsMultipleGroupsNoRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the tags fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" },
				{ "AudioLeft", dataset.getDatasetUuid().toString(), "datasets" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] tagValues = { { "EyeTrack", "VisualTarget" },
				{ "AudioLeft" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET') INTERSECT SELECT TAG_ENTITY_UUID FROM TAGS WHERE UPPER(TAG_NAME) IN('AUDIOLEFT'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsTagsMultipleGroupsRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the tags fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" },
				{ "AudioLeft", dataset.getDatasetUuid().toString(), "datasets" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] tagValues = { { "Eye*", "Visual*" }, { "Audio*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", tagValues, null, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'EYE*|VISUAL*' INTERSECT SELECT TAG_ENTITY_UUID FROM TAGS WHERE TAG_NAME ~* 'AUDIO*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsAttributesSingleGroupNoRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] attributeValues = { { "ALPHA", "BETA" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", null, attributeValues, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN ('ALPHA', 'BETA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsAttributesSingleGroupRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] attributeValues = { { "A*", "B*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", null, attributeValues, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsAttributesMultipleGroupsNoRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "3", null, "Omega" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] attributeValues = { { "ALPHA", "BETA" }, { "Omega" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", null, attributeValues, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN ('ALPHA', 'BETA') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN ('OMEGA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsAttributesMultipleGroupsRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "VERSION_TEST_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "3", null, "Omega" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[][] attributeValues = { { "A*", "B*" }, { "O*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", null, attributeValues, null, null);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'O*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureAttributesNoRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure and attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "attribute_dataset",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "3", null, "Omega" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[] columnNames3 = { "dataset_name" };
		String[][] columnValues3 = { { "attribute_dataset" } };
		String[][] attributeValues = { { "Alpha", "Beta" }, { "Omega" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", null, attributeValues, columnNames3, columnValues3);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT dataset_uuid from DATASETS WHERE UPPER(DATASET_NAME) IN('ATTRIBUTE_DATASET') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('ALPHA','BETA') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('OMEGA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureAttributesRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure and attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "attribute_dataset",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "3", null, "Omega" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[] columnNames3 = { "dataset_name" };
		String[][] columnValues3 = { { "attribute*" } };
		String[][] attributeValues = { { "A*", "B*" }, { "O*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", null, attributeValues, columnNames3, columnValues3);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT dataset_uuid from DATASETS WHERE DATASET_NAME ~* 'attribute*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'O*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsNoRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure and tag fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "tag_dataset",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[] columnNames3 = { "dataset_name" };
		String[][] columnValues3 = { { "tag_dataset" } };
		String[][] tagValues = { { "EyeTrack", "VisualTarget" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", tagValues, null, columnNames3, columnValues3);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE UPPER(DATASET_NAME) IN('TAG_DATASET') INTERSECT SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsRegExp() throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the tags fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "tag_dataset",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[] columnNames3 = { "dataset_name" };
		String[][] columnValues3 = { { "tag*" } };
		String[][] tagValues = { { "Eye*", "Visual*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", tagValues, null, columnNames3, columnValues3);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE DATASET_NAME ~* 'tag*' INTERSECT SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'Eye*|Visual*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsAttributesNoRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure, tag, and attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "tag_attribute_dataset",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "3", null, "Omega" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[] columnNames3 = { "dataset_name" };
		String[][] columnValues3 = { { "tag_attribute_dataset" } };
		String[][] tagValues = { { "EyeTrack", "VisualTarget" } };
		String[][] attributeValues = { { "Alpha", "Beta" }, { "Omega" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"off", tagValues, attributeValues, columnNames3, columnValues3);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE UPPER(DATASET_NAME) IN('TAG_ATTRIBUTE_DATASET') INTERSECT SELECT tag_entity_uuid from TAGS WHERE UPPER(TAG_NAME) IN ('EYETRACK', 'VISUALTARGET')INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('ALPHA','BETA') INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE) IN('OMEGA'))";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsStructureTagsAttributesRegExp()
			throws Exception {
		System.out
				.println("TEST: testing retrieveRows() method. The retireveRows method should retireve all rows based on the structure, tag, and attribute fields passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		Datasets dataset = new Datasets(md.getConnection());
		dataset.reset(false, "tag_attribute_dataset",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for version", "VERSION_TEST_NAMESPACE", UUID
						.randomUUID().toString(), null);
		dataset.save();
		tableName = "tags";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues = {
				{ "EyeTrack", dataset.getDatasetUuid().toString(), "datasets" },
				{ "VisualTarget", dataset.getDatasetUuid().toString(),
						"datasets" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
		tableName = "attributes";
		columnNames = md.getColumnNames(tableName);
		String[][] columnValues2 = {
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "1", null, "Alpha" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "2", null, "Beta" },
				{ null, UUID.randomUUID().toString(),
						dataset.getDatasetUuid().toString(),
						UUID.randomUUID().toString(), "3", null, "Omega" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		md.commit();
		tableName = "datasets";
		String[] columnNames3 = { "dataset_name" };
		String[][] columnValues3 = { { "tag*" } };
		String[][] tagValues = { { "Eye*", "Visual*" } };
		String[][] attributeValues = { { "A*", "B*" }, { "O*" } };
		String[][] rows = md.retrieveRows(tableName, Double.POSITIVE_INFINITY,
				"on", tagValues, attributeValues, columnNames3, columnValues3);
		assertNotNull(rows);
		int actual = rows.length;
		Statement st = md.getConnection().createStatement();
		String qry = "SELECT * FROM "
				+ tableName
				+ " WHERE dataset_uuid IN (SELECT DATASET_UUID FROM DATASETS WHERE DATASET_NAME ~* 'tag_attribute*' INTERSECT SELECT tag_entity_uuid from TAGS WHERE TAG_NAME ~* 'Eye*|Visual*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'A*|B*' INTERSECT SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
				+ " FROM attributes WHERE ATTRIBUTE_VALUE ~* 'O*')";
		ResultSet rs = st.executeQuery(qry);
		int expected = 0;
		while (rs.next())
			expected++;
		assertEquals(
				"Number of rows returned is not equal to the excpected row",
				expected, actual);
	}

	@Test
	public void testRetrieveRowsByRangeNoCriteria() throws Exception {
		String tableName = "event_types";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { null, "light flash", "description" },
				{ null, "audio stimulus", "description" },
				{ null, "button press", "description" } };
		String[] keys = md.addRows(tableName, columnNames, columnValues, null,
				null);
		tableName = "events";
		columnNames = md.getColumnNames(tableName);
		String entityUuid = UUID.randomUUID().toString();
		String[][] columnValues2 = {
				{ null, entityUuid, keys[0], "1", "1", "1", "1.0" },
				{ null, entityUuid, keys[1], "2", "2", "2", "1.0" },
				{ null, entityUuid, keys[2], "3", "3", "3", "1.0" } };
		md.addRows(tableName, columnNames, columnValues2, null, null);
		String[][] rows = md.extractRows("events", null, null, "events", null,
				null, Double.POSITIVE_INFINITY, "off", 0, 1);
		int actual = rows.length;
		String qry = "SELECT * FROM extractRange('SELECT * FROM events','SELECT * FROM events',0.0,1.0) as (event_uuid uuid,event_entity_uuid uuid,event_type_uuid uuid,event_start_time double precision,event_end_time double precision,event_position bigint,event_certainty double precision,extracted uuid[]) ORDER BY EVENT_ENTITY_UUID, EVENT_START_TIME";
		Statement stmt = md.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(qry);
		int expected = 0;
		while (rs.next()) {
			expected++;
		}
		assertEquals(
				"The number of rows returned is not equal to the expected",
				expected, actual);

	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidTableName() throws Exception {
		System.out
				.println("TEST: testing addRows() method. The addRows method should throw an exception when an invalid table name is passed in");
		String tableName = "invalid_table";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidColumnNames() throws Exception {
		System.out
				.println("TEST: testing addRows() method. The addRows method should throw an exception when invalid rows are passed in");
		String tableName = "tags";
		String[] columnNames = { "invalid_column1", "invalid_column2",
				"invalid_column3" };
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidColumnValues() throws Exception {
		System.out
				.println("TEST: testing addRows() method. The addRows method should throw an exception when invalid rows are passed in");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "invalid_value1", "invalid_value2",
				"invalid_value3" } };
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@After
	public void cleanAfterTest() throws Exception {
		if (md.getConnection() != null)
			md.close();
	}

}
