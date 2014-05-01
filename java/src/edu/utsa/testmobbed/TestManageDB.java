/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.MobbedException;

/**
 * Unit tests for ManageDB class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestManageDB {
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "managedb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testAddRowsCompositeKey() throws Exception {
		System.out
				.println("Unit test for addRows with table with a composite primary key");
		String tableName = "tags";
		String[] columnNames = md.getColumnNames(tableName);
		String tagUuid = UUID.randomUUID().toString();
		String[][] tagColumnValues = { { tagUuid, "tag1", } };
		md.addRows(tableName, columnNames, tagColumnValues, null, null);
		tableName = "tag_entities";
		String[][] tagEntityColumnValues = { { UUID.randomUUID().toString(),
				tagUuid, "datasets" } };
		columnNames = md.getColumnNames(tableName);
		String[] actual = md.addRows(tableName, columnNames,
				tagEntityColumnValues, null, null);
		assertNotNull(actual);
		String[] expected = { tagEntityColumnValues[0][0] + ","
				+ tagEntityColumnValues[0][1] };
		System.out
				.println("--It should return a comma separated composite primary key for the given table tag_entities\n");
		assertArrayEquals(
				"The composite key for the tag_entities table is not returned",
				expected, actual);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidColumnNames() throws Exception {
		System.out.println("Unit test for addRows with invalid columns names");
		String tableName = "tags";
		String[] columnNames = { "invalid_column1", "invalid_column2",
				"invalid_column3" };
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		System.out
				.println("--It should throw an exception when specifying an invalid column name\n");
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidColumnValues() throws Exception {
		System.out.println("Unit test for addRows with invalid columns values");
		String tableName = "datasets";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "invalid_value1", "invalid_value2",
				"invalid_value3" } };
		System.out
				.println("--It should throw an exception when specifying an invalid column value\n");
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testAddRowsInvalidTableName() throws Exception {
		System.out.println("Unit test for addRows with invalid table");
		String tableName = "invalid_table";
		String[] columnNames = md.getColumnNames(tableName);
		String[][] columnValues = { { "tag1", UUID.randomUUID().toString(),
				"datasets" } };
		System.out
				.println("--It should throw an exception when specifying an invalid table\n");
		md.addRows(tableName, columnNames, columnValues, null, null);
	}

	@Test(expected = MobbedException.class)
	public void testCloseInvalidCursor() throws Exception {
		md.closeCursor("invalid_cursor");
	}

	@Test
	public void testGetColumnNames() throws Exception {
		System.out.println("Unit test for getColumnNames");
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
				.println("--It should return the expected column names for the given table datasets\n");
		assertArrayEquals(
				"The columns from the datasets table are not returned",
				expected, actual);
	}

	@Test
	public void testGetColumnType() throws Exception {
		System.out.println("Unit test for getColumnType");
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
				.println("--It should return the expected column type for the given column dataset_namespace\n");
		assertEquals(
				"The column type from the dataset_namespace column is not returned",
				expected, actual);
	}

	@Test
	public void testgetColumnTypes() throws Exception {
		System.out.println("Unit test for getColumnTypes");
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
				.println("--It should return the expected column types for the given table datasets\n");
		assertArrayEquals(
				"The column types from the datasets table are not returned",
				expected, actual);
	}

	@Test
	public void testGetDefaultValue() throws Exception {
		System.out.println("Unit test for getDefaultValue");
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
				.println("--It should return the expected default value for the given column dataset_namespace\n");
		assertEquals(
				"The default value for the column dataset_namespace is not returned",
				expected, actual);
	}

	@Test
	public void testGetDoubleColumns() throws Exception {
		System.out.println("Unit test for getDoubleColumns");
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
				.println("--It should return the expected double columns for the given table attributes\n");
		assertArrayEquals(
				"The double columns from the attributes table are not returned",
				expected, actual);
	}

	@Test
	public void testGetKeys() throws Exception {
		System.out.println("Unit test for getKeys");
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
				.println("--It should return the expected keys for the given table datasets\n");
		assertArrayEquals("The keys from the datasets table are not returned",
				expected, actual);
	}

	@Test
	public void testgetTables() throws Exception {
		System.out.println("Unit test for getTables");
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
				.println("--It should return all of the tables in the database\n");
		assertArrayEquals("All tables in the database are not returned",
				expected, actual);
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
		}
		md.setAutoCommit(true);
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.closeConnection();
		ManageDB.dropDatabase(name, hostname, user, password, verbose);

	}

}
