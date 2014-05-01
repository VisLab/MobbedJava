package edu.utsa.mobbed;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.postgresql.largeobject.LargeObjectManager;

/**
 * Handler class for MOBBED database. This handler can be used to update,
 * insert, and query the database.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */

public class ManageDB {
	/**
	 * A HashMap that contains the column names of each database table
	 */
	private HashMap<String, String[]> colMap = new HashMap<String, String[]>();
	/**
	 * A connection to the database
	 */
	private Connection con;
	/**
	 * A HashMap that contains the default column values of each database table
	 */
	private HashMap<String, String> defaultVals = new HashMap<String, String>();
	/**
	 * A HashMap that contains the keys of each database table
	 */
	private HashMap<String, String[]> keyMap = new HashMap<String, String[]>();
	/**
	 * A HashMap that contains the column types of each database table
	 */
	private HashMap<String, String> typeMap = new HashMap<String, String>();
	/**
	 * prints informative messages if true
	 */
	private boolean verbose = true;
	/**
	 * The default contact uuid
	 */
	public static final String DEFAULT_CONTACT_UUID = "591df7dd-ce3e-47f8-bea5-6a632c6fcccb";
	/**
	 * The default CSV modality uuid
	 */
	public static final String DEFAULT_CSV_MODALITY_UUID = "991df7dd-ce3e-47f8-bea5-6a632c6fcccb";
	/**
	 * The default EEG modality uuid
	 */
	public static final String DEFAULT_EEG_MODALITY_UUID = "691df7dd-ce3e-47f8-bea5-6a632c6fcccb";
	/**
	 * The default GENERIC modality uuid
	 */
	public static final String DEFAULT_GENERIC_MODALITY_UUID = "791df7dd-ce3e-47f8-bea5-6a632c6fcccb";
	/**
	 * The default SIMPLE modality uuid
	 */
	public static final String DEFAULT_SIMPLE_MODALITY_UUID = "891df7dd-ce3e-47f8-bea5-6a632c6fcccb";
	/**
	 * The uuid representing an entity with no parent
	 */
	public static final String NO_PARENT_UUID = "491df7dd-ce3e-47f8-bea5-6a632c6fcccb";
	/**
	 * A HashMap that contains tag names and tag uuids
	 */
	public static HashMap<String, UUID> tagMap = new HashMap<String, UUID>();
	/**
	 * A query that retrieves column metadata of a database table
	 */
	private static final String colQuery = "SELECT column_default, column_name, data_type from information_schema.columns where table_schema = 'public' AND table_name = ?";
	/**
	 * A HashMap that contains instances of ManageDB objects
	 */
	private static HashMap<ManageDB, String> dbMap = new HashMap<ManageDB, String>();
	/**
	 * A query that retrieves the keys of a database table
	 */
	private static final String keyQuery = "SELECT pg_attribute.attname FROM pg_index, pg_class, pg_attribute"
			+ " WHERE pg_class.oid = ?::regclass AND"
			+ " indrelid = pg_class.oid AND"
			+ " pg_attribute.attrelid = pg_class.oid AND"
			+ " pg_attribute.attnum = any(pg_index.indkey)"
			+ " AND indisprimary";
	/**
	 * A query that retrieves the tables of a database
	 */
	private static final String tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
	/**
	 * The name of the template database
	 */
	private static final String templateDatabase = "template1";

	/**
	 * Creates a ManageDB object.
	 * 
	 * @param name
	 *            the name of the database
	 * @param hostname
	 *            the host name of the database
	 * @param username
	 *            the user name of the database
	 * @param password
	 *            the password of the database
	 * @param verbose
	 *            prints informative messages if true
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public ManageDB(String name, String hostname, String username,
			String password, boolean verbose) throws MobbedException {
		con = establishConnection(name, hostname, username, password);
		this.verbose = verbose;
		setAutoCommit(false);
		populateHashMaps();
		addManageDB(this);
	}

	/**
	 * Inserts or updates rows in the database. To insert rows, do not assign
	 * values to the key columns. To update rows, assign values to the key
	 * columns that already exist in the database.
	 * 
	 * @param table
	 *            the name of the database table
	 * @param cols
	 *            the names of the non-double database columns
	 * @param vals
	 *            the values of the non-double database columns
	 * @param dcols
	 *            the names of the double database columns database table
	 * @param dvals
	 *            the values of the double database columns
	 * @return the keys of the database rows that were inserted and/or updated
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addRows(String table, String[] cols, String[][] vals,
			String[] dcols, Double[][] dvals) throws MobbedException {
		validateTableName(table);
		validateColumns(cols);
		int numRows = vals.length;
		int numValues = vals[0].length;
		int doubleIndex = 0;
		Double[] currentDoubleValues;
		String[] keyList = new String[numRows];
		ArrayList<Integer> keyIndexes = findKeyIndexes(table, cols);
		String insertQry = constructInsertQuery(table, cols);
		String updateQry = constructUpdateQuery(keyIndexes, table, cols);
		try {
			PreparedStatement insertStmt = con.prepareStatement(insertQry);
			PreparedStatement updateStmt = con.prepareStatement(updateQry);
			for (int i = 0; i < numRows; i++) {
				for (int j = 0; j < numValues; j++) {
					if (!isEmpty(vals[i][j]))
						validateValues(cols[j], vals[i][j]);
					else {
						if (!keyIndexes.contains(j)
								&& !cols[j].equals("dataset_session_uuid"))
							vals[i][j] = getDefaultValue(cols[j]);
					}
				}
				if (!isEmpty(dvals))
					currentDoubleValues = dvals[doubleIndex++];
				else
					currentDoubleValues = null;
				if (keysExist(keyIndexes, table, cols, vals[i])) {
					setUpdateStatementValues(keyIndexes, updateStmt, cols,
							vals[i], currentDoubleValues);
					if (verbose)
						System.out.println(updateStmt);
					updateStmt.addBatch();
				} else {
					vals[i] = generateKeys(keyIndexes, table, cols, vals[i]);
					setInsertStatementValues(insertStmt, cols, vals[i],
							currentDoubleValues);
					if (verbose)
						System.out.println(insertStmt);
					insertStmt.addBatch();
				}
				keyList[i] = addKeyValue(keyIndexes, vals[i]);
			}
			insertStmt.executeBatch();
			updateStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not insert or update row(s) in the database\n"
							+ ex.getNextException().getMessage());
		}
		return keyList;
	}

	/**
	 * Checks the dataset namespace and name combination. If isUnique is set to
	 * true and the combination already exist then an exception will be thrown.
	 * If isUnique is false and the combination already exist then the dataset
	 * version will be incremented signifying a duplicate dataset.
	 * 
	 * @param isUnique
	 *            if the dataset namespace and name combination is unique
	 *            combination
	 * @param datasetName
	 *            the name of the dataset
	 * @param datasetNamespace
	 *            the namespace of the dataset
	 * @return the version number of the dataset
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public int checkDatasetVersion(boolean isUnique, String namespace,
			String name) throws MobbedException {
		String qry = "SELECT MAX(DATASET_VERSION) AS LATESTVERSION"
				+ " FROM DATASETS WHERE DATASET_NAMESPACE = ? AND DATASET_NAME = ?";
		int version;
		try {
			PreparedStatement smt = con.prepareStatement(qry);
			smt.setString(1, namespace);
			smt.setString(2, name);
			ResultSet rs = smt.executeQuery();
			rs.next();
			version = rs.getInt(1);
			if (isUnique && version > 0)
				throw new MobbedException("dataset version is not unique");
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not execute query to determine dataset version\n"
							+ ex.getMessage());
		}
		return version + 1;
	}

	/**
	 * Closes a database connection.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void closeConnection() throws MobbedException {
		try {
			con.close();
			removeManageDB(this);
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not close the database connection\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Closes a data cursor. The data cursor must be open prior to closing it.
	 * 
	 * @param name
	 *            the name of the data cursor
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void closeCursor(String name) throws MobbedException {
		String qry = "CLOSE " + name;
		try {
			Statement smt = con.createStatement();
			smt.execute(qry);
		} catch (SQLException ex) {
			throw new MobbedException("Could not close the data cursor\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Commits the current database transaction.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void commitTransaction() throws MobbedException {
		try {
			con.commit();
		} catch (SQLException ex) {
			throw new MobbedException("Could not commit transaction\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Gets the auto commit mode.
	 * 
	 * @return the state of auto commit mode
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public boolean getAutoCommit() throws MobbedException {
		try {
			return con.getAutoCommit();
		} catch (SQLException ex) {
			throw new MobbedException("Could not get auto commit mode");
		}
	}

	/**
	 * Gets the column names of a database table.
	 * 
	 * @param table
	 *            the name of the database table
	 * @return the columns of the table
	 */
	public String[] getColumnNames(String table) {
		return colMap.get(table.toLowerCase());
	}

	/**
	 * Gets the type of a database column.
	 * 
	 * @param col
	 *            the name of the database column
	 * @return the type of the column
	 */
	public String getColumnType(String col) {
		return typeMap.get(col.toLowerCase());
	}

	/**
	 * Gets all of the columns types from a database table.
	 * 
	 * @param table
	 *            the name of the database table
	 * @return the types of the columns in the table
	 */
	public String[] getColumnTypes(String table) {
		String[] cols = getColumnNames(table.toLowerCase());
		int numCols = cols.length;
		String[] colTypes = new String[numCols];
		for (int i = 0; i < numCols; i++)
			colTypes[i] = getColumnType(cols[i]);
		return colTypes;
	}

	/**
	 * Gets a database connection.
	 * 
	 * @return a database connection
	 */
	public Connection getConnection() {
		return con;
	}

	/**
	 * Gets the default value of a database column.
	 * 
	 * @param col
	 *            the name of the database column
	 * @return the default value of a column
	 */
	public String getDefaultValue(String col) {
		return defaultVals.get(col.toLowerCase());
	}

	/**
	 * Gets the columns that are double precision of a database table.
	 * 
	 * @param table
	 *            the name of the database table
	 * @return the double columns in the database table
	 */
	public String[] getDoubleColumns(String table) {
		ArrayList<String> al = new ArrayList<String>();
		String[] cols = colMap.get(table.toLowerCase());
		int numCols = cols.length;
		for (int i = 0; i < numCols; i++) {
			if (typeMap.get(cols[i]).equalsIgnoreCase("double precision"))
				al.add(cols[i]);
		}
		String[] dCols = al.toArray(new String[al.size()]);
		return dCols;
	}

	/**
	 * Gets the keys of a database table.
	 * 
	 * @param table
	 *            the name of the database table
	 * @return the keys in the table
	 */
	public String[] getKeys(String table) {
		return keyMap.get(table.toLowerCase());
	}

	/**
	 * Gets all of the tables from the database.
	 * 
	 * @return the tables from the database
	 */
	public String[] getTables() {
		Set<String> keySet = colMap.keySet();
		String[] tables = keySet.toArray(new String[keySet.size()]);
		return tables;
	}

	/**
	 * Returns a HashMap with all tags in the database
	 * 
	 * @return
	 */
	public HashMap<String, UUID> getTagMap() {
		return tagMap;
	}

	/**
	 * Rollback the current transaction. Auto commit mode needs to be set to
	 * false to create a transaction.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void rollbackTransaction() throws MobbedException {
		try {
			con.rollback();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not rollback current transaction\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Retrieves rows from the database based on search criteria
	 * 
	 * @param table
	 *            the name of the database table
	 * @param limit
	 *            the maximum number of rows to retrieve
	 * @param regex
	 *            on if regular expressions are enable, off if disabled
	 * @param match
	 *            the type of tag match
	 * @param tags
	 *            the tags search criteria
	 * @param atts
	 *            the attributes search criteria
	 * @param cols
	 *            the names of the non-double database columns
	 * @param vals
	 *            the values of the non-double database columns
	 * @param dcols
	 *            the names of the double database columns
	 * @param dvals
	 *            the values of the double database columns
	 * @param range
	 *            the range to search by double database columns
	 * @param cursor
	 *            the name of the data cursor
	 * @return the rows found by the search criteria
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[][] searchRows(String table, double limit, String regex,
			String match, String[][] tags, String[][] atts, String[] cols,
			String[][] vals, String[] dcols, Double[][] dvals,
			double[][] range, String cursor) throws MobbedException {
		validateTableAndColumns(table, cols, dcols);
		String searchQuery = constructSearchQuery(table, limit, regex, match,
				tags, atts, cols, vals, dcols, dvals, range, cursor);
		return returnSearchRows(searchQuery, cursor, (int) limit);
	}

	/**
	 * Sets the auto commit mode of the connection.
	 * 
	 * @param mode
	 *            true to enable autocommit mode, false to disable it
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void setAutoCommit(boolean mode) throws MobbedException {
		try {
			con.setAutoCommit(mode);
		} catch (SQLException ex) {
			throw new MobbedException("Could not set the auto commit mode\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Adds elements to an array by index.
	 * 
	 * @param keyIndexes
	 *            a list of the database key indexes
	 * @param cols
	 *            the names of the database columns
	 * @return a array that contains the key columns
	 */
	private String[] addByIndex(ArrayList<Integer> keyIndexes, String[] cols) {
		String[] newArray = new String[keyIndexes.size()];
		int j = 0;
		for (int i = 0; i < keyIndexes.size(); i++) {
			if (keyIndexes.contains(i)) {
				newArray[j] = cols[i];
				j++;
			}
		}
		return newArray;
	}

	/**
	 * Adds a key value to a array.
	 * 
	 * @param keyIndexes
	 *            a list of the database key indexes
	 * @param vals
	 *            the values of the database columns
	 * @return a key value
	 */
	private String addKeyValue(ArrayList<Integer> keyIndexes, String[] vals) {
		String keyValue = null;
		if (keyIndexes.size() == 1)
			keyValue = vals[keyIndexes.get(0)];
		else {
			String concatKeys = vals[keyIndexes.get(0)];
			for (int i = 1; i < keyIndexes.size(); i++)
				concatKeys += "," + vals[keyIndexes.get(i)];
			keyValue = concatKeys;
		}
		return keyValue;
	}

	private String concatLimit(String qry, String name, double limit) {
		if (isEmpty(name) && limit != Double.POSITIVE_INFINITY) {
			qry = concatStrs(qry, "LIMIT " + (int) limit);
		}
		return qry;
	}

	/**
	 * Constructs a insert query.
	 * 
	 * @param table
	 *            the name of the database table
	 * @param cols
	 *            the names of the database columns
	 * @return a insert query string
	 */
	private String constructInsertQuery(String table, String[] cols) {
		String qry = "INSERT INTO " + table;
		qry += " (" + cols[0];
		for (int i = 1; i < cols.length; i++)
			qry += ", " + cols[i];
		qry += ")";
		qry += " VALUES (?";
		for (int j = 1; j < cols.length; j++)
			qry += ", ?";
		qry += ")";
		return qry;
	}

	/**
	 * Constructs a query used to retrieve database rows
	 * 
	 * @param table
	 *            the name of the database table
	 * @param limit
	 *            the maximum number of rows to retrieve
	 * @param regex
	 *            on if regular expressions are enable, off if disabled
	 * @param match
	 *            the type of tag match
	 * @param tags
	 *            the tags search criteria
	 * @param atts
	 *            the attributes search criteria
	 * @param cols
	 *            the names of the non-double database columns
	 * @param vals
	 *            the values of the non-double database columns
	 * @param dcols
	 *            the names of the double database columns
	 * @param dvals
	 *            the values of the double database columns
	 * @param range
	 *            the range to search by double database columns
	 * @return
	 */
	private String constructSearchQuery(String table, double limit,
			String regex, String match, String[][] tags, String[][] atts,
			String[] cols, String[][] vals, String[] dcols, Double[][] dvals,
			double[][] range, String cursorName) throws MobbedException {
		String qry = "SELECT " + table.toUpperCase() + ".* FROM "
				+ table.toUpperCase();
		qry = concatStrs(qry, GroupQuery.constructGroupQuery(con, table,
				keyMap.get(table)[0], "TAGS", match, tags));
		qry = concatStrs(qry, GroupQuery.constructGroupQuery(con, table,
				keyMap.get(table)[0], "ATTRIBUTES", regex, atts));
		qry = concatStrs(qry, EntityQuery.constructQuery(this, regex, cols,
				vals, dcols, dvals, range));
		qry = concatLimit(qry, cursorName, limit);
		System.out.println(qry);
		return qry;
	}

	/**
	 * Constructs a select query.
	 * 
	 * @param keyIndexes
	 *            a list of the database key indexes
	 * @param table
	 *            the name of the database table
	 * @param columns
	 *            the names of the database columns
	 * @return a query string
	 */
	private String constructSelectQuery(ArrayList<Integer> keyIndexes,
			String table, String[] columns) {
		String[] keyColumns = addByIndex(keyIndexes, columns);
		String qry = "SELECT * FROM " + table + " WHERE " + keyColumns[0]
				+ " = ?";
		for (int i = 1; i < keyColumns.length; i++) {
			qry += " AND " + keyColumns[i] + " = ?";
		}
		return qry;
	}

	/**
	 * Constructs a update query.
	 * 
	 * @param keyIndexes
	 *            a list of the key indexes
	 * @param table
	 *            the name of the database table
	 * @param cols
	 *            the names of the database columns
	 * @return a update query string
	 */
	private String constructUpdateQuery(ArrayList<Integer> keyIndexes,
			String table, String[] cols) {
		String[] nonKeyCols = removeByIndex(keyIndexes, cols);
		String[] keyCols = addByIndex(keyIndexes, cols);
		String qry = "UPDATE " + table + " SET " + nonKeyCols[0] + " = ?";
		for (int i = 1; i < nonKeyCols.length; i++)
			qry += " , " + nonKeyCols[i] + " = ?";
		qry += " WHERE " + keyCols[0] + " = ?";
		for (int j = 1; j < keyCols.length; j++)
			qry += " AND " + keyCols[j] + " = ?";
		return qry;
	}

	/**
	 * Creates a data cursor
	 * 
	 * @param cursor
	 *            the name of the data cursor
	 * @param qry
	 *            the query the data cursor will be bounded to
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void createCursor(String cursor, String qry) throws MobbedException {
		qry = "DECLARE " + cursor + " SCROLL CURSOR WITH HOLD FOR " + qry;
		try {
			Statement smt = con.createStatement();
			smt.execute(qry);
			if (verbose)
				System.out.println(qry);
		} catch (SQLException ex) {
			throw new MobbedException("Could not create data cursor\n"
					+ ex.getMessage());
		}

	}

	/**
	 * 
	 * @param cursor
	 *            the name of the cursor
	 * @return true if the cursor exists, false if otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean cursorExists(String cursor) throws MobbedException {
		boolean cursorExists = false;
		String qry = "SELECT EXISTS (SELECT 1 FROM PG_CURSORS WHERE NAME = ?)";
		try {
			PreparedStatement smt = con.prepareStatement(qry);
			smt.setString(1, cursor);
			ResultSet rs = smt.executeQuery();
			rs.next();
			cursorExists = rs.getBoolean(1);
		} catch (SQLException ex) {
			throw new MobbedException("Could not check if data cursor exists\n"
					+ ex.getMessage());
		}
		return cursorExists;
	}

	/**
	 * Fetches the next set of rows that a data cursor points to
	 * 
	 * @param cursor
	 *            the name of the cursor
	 * @param size
	 *            the fetch size of the cursor
	 * @return a set of rows that the cursor fetches
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String[][] fetchFromCursor(String cursor, int size)
			throws MobbedException {
		String qry = "FETCH FORWARD " + size + " FROM " + cursor;
		String[][] rows = putRowsInArray(qry);
		if (isEmpty(rows))
			closeCursor(cursor);
		return rows;
	}

	/**
	 * Finds the index of a particular column.
	 * 
	 * @param cols
	 *            the names of the database columns
	 * @param name
	 *            the name of the database column that the index is searched for
	 * @return the index of the database column name
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private int findColumnIndex(String[] cols, String name)
			throws MobbedException {
		int index = 0;
		boolean found = false;
		int numCols = cols.length;
		for (int i = 0; i < numCols; i++) {
			if (cols[i].equalsIgnoreCase(name)) {
				index = i;
				found = true;
			}
		}
		if (!found)
			throw new MobbedException("Could not find the index of the column "
					+ name);
		return index;
	}

	/**
	 * Finds the indexes that contain key columns.
	 * 
	 * @param table
	 *            the name of the database table
	 * @param cols
	 *            the names of the database columns
	 * @return the indexes of the key columns
	 */
	private ArrayList<Integer> findKeyIndexes(String table, String[] cols) {
		String[] keys = keyMap.get(table);
		ArrayList<Integer> keyIndexes = new ArrayList<Integer>();
		for (int i = 0; i < cols.length; i++) {
			for (int j = 0; j < keys.length; j++)
				if (cols[i].equalsIgnoreCase(keys[j]))
					keyIndexes.add(i);
		}
		return keyIndexes;
	}

	/**
	 * Generates the keys for insertion.
	 * 
	 * @param keyIndexes
	 *            a list of key indexes
	 * @param table
	 *            the name of the database table
	 * @param cols
	 *            the names of the database columns
	 * @param vals
	 *            the values of the databse columns
	 * @return keys for each row being inserted
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String[] generateKeys(ArrayList<Integer> keyIndexes, String table,
			String[] cols, String[] vals) throws MobbedException {
		if (table.equalsIgnoreCase("datasets")) {
			int sessionIndex = findColumnIndex(cols, "dataset_session_uuid");
			vals[sessionIndex] = UUID.randomUUID().toString();
		}
		int numKeys = keyIndexes.size();
		for (int i = 0; i < numKeys; i++) {
			if (isEmpty(vals[keyIndexes.get(i)]))
				vals[keyIndexes.get(i)] = UUID.randomUUID().toString();
		}
		return vals;
	}

	/**
	 * Looks up the jdbc sql types of a given column.
	 * 
	 * @param col
	 *            the name of the database column
	 * @return the jdbc sql type of the column
	 */
	private int getJDBCType(Object col) {
		// 1.6 doesn't support switch statement with string object
		String type = typeMap.get(col);
		int targetType = Types.NULL;
		if (type.equalsIgnoreCase("uuid")) {
			targetType = Types.OTHER;
		} else if (type.equalsIgnoreCase("character varying")) {
			targetType = Types.VARCHAR;
		} else if (type.equalsIgnoreCase("ARRAY")) {
			targetType = Types.ARRAY;
		} else if (type.equalsIgnoreCase("integer")) {
			targetType = Types.INTEGER;
		} else if (type.equalsIgnoreCase("bigint")) {
			targetType = Types.BIGINT;
		} else if (type.equalsIgnoreCase("double precision")) {
			targetType = Types.DOUBLE;
		} else if (type.equalsIgnoreCase("timestamp without time zone")) {
			targetType = Types.OTHER;
		} else if (type.equalsIgnoreCase("oid")) {
			targetType = Types.INTEGER;
		}
		return targetType;
	}

	/**
	 * Initializes the hashmaps that contain column metadata.
	 * 
	 * @param smt
	 *            the prepared statement object used for the queries
	 * @param table
	 *            the name of the database table
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void initializeColumnHashMaps(PreparedStatement smt, String table)
			throws MobbedException {
		ArrayList<String> colList = new ArrayList<String>();
		String colDefault = null;
		String colName = null;
		String colType = null;
		try {
			smt.setString(1, table);
			ResultSet rs = smt.executeQuery();
			while (rs.next()) {
				colDefault = rs.getString(1);
				if (colDefault != null)
					colDefault = colDefault.split(":")[0].replaceAll("'", "");
				colName = rs.getString(2);
				colType = rs.getString(3);
				defaultVals.put(colName, colDefault);
				typeMap.put(colName, colType);
				colList.add(colName);
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve the columns\n"
					+ ex.getMessage());
		}
		String[] cols = colList.toArray(new String[colList.size()]);
		colMap.put(table, cols);
	}

	/**
	 * Initializes a HashMap that contains the keys of each table.
	 * 
	 * @param keyStatement
	 *            the prepared statement object used for the queries
	 * @param table
	 *            the name of the database table
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void initializeKeyHashMap(PreparedStatement keyStatement,
			String table) throws MobbedException {
		ArrayList<String> keyColumnList = new ArrayList<String>();
		try {
			keyStatement.setString(1, table);
			ResultSet rs = keyStatement.executeQuery();
			while (rs.next())
				keyColumnList.add(rs.getString(1));
		} catch (SQLException ex) {
			throw new MobbedException("Could not initialize key HashMap\n"
					+ ex.getMessage());
		}
		String[] keyColumns = keyColumnList.toArray(new String[keyColumnList
				.size()]);
		keyMap.put(table, keyColumns);
	}

	/**
	 * Checks if key columns are empty.
	 * 
	 * @param keyIndexes
	 *            a list of key indexes
	 * @param values
	 *            the values of the database columns
	 * @return true if the key columns are empty, false if otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean keysEmpty(ArrayList<Integer> keyIndexes, String[] values)
			throws MobbedException {
		boolean empty = true;
		int keyCount = 0;
		for (int i = 0; i < keyIndexes.size(); i++) {
			if (!isEmpty(values[keyIndexes.get(i)]))
				keyCount++;
		}
		if (keyCount > 0 && keyIndexes.size() > keyCount)
			throw new MobbedException(
					"All composite key values must be provided");
		else if (keyCount == keyIndexes.size())
			empty = false;
		return empty;
	}

	/**
	 * Checks if the given keys exist in the database.
	 * 
	 * @param keyIndexes
	 *            a list of key indexes
	 * @param table
	 *            the name of the database table
	 * @param columns
	 *            the names of the database columns
	 * @param values
	 *            the values of the database columns
	 * @return true if the keys exist in the database, false if otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean keysExist(ArrayList<Integer> keyIndexes, String table,
			String[] columns, String[] values) throws MobbedException {
		boolean exist = false;
		if (!keysEmpty(keyIndexes, values)) {
			String[] keyColumns = addByIndex(keyIndexes, columns);
			String[] keyValues = addByIndex(keyIndexes, values);
			String selectQuery = constructSelectQuery(keyIndexes, table,
					columns);
			try {
				PreparedStatement pstmt = con.prepareStatement(selectQuery);
				setSelectStatementValues(pstmt, keyColumns, keyValues);
				ResultSet rs = pstmt.executeQuery();
				if (rs.next())
					exist = true;
			} catch (SQLException ex) {
				throw new MobbedException(
						"Could not execute query to find if keys exist\n"
								+ ex.getMessage());
			}
		}
		return exist;
	}

	/**
	 * Populates the HashMaps. The HashMaps contain metadata about columns and
	 * the keys from each table.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void populateHashMaps() throws MobbedException {
		ResultSet rs = null;
		PreparedStatement colSmt = null;
		PreparedStatement keySmt = null;
		try {
			tagMap = InitializeTagMap(con);
			Statement tableSmt = con.createStatement();
			colSmt = con.prepareCall(colQuery);
			keySmt = con.prepareCall(keyQuery);
			rs = tableSmt.executeQuery(tableQuery);
			while (rs.next()) {
				initializeColumnHashMaps(colSmt, rs.getString("table_name"));
				initializeKeyHashMap(keySmt, rs.getString("table_name"));
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not initialize statement objects\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Populates an array with a result set.
	 * 
	 * @param rs
	 *            the result set object that contains the rows from the query
	 * @return an array that mirrors the rows in the result set
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String[][] putRowsInArray(String qry) throws MobbedException {
		String[][] allocatedArray = {};
		try {
			Statement stmt = con.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = stmt.executeQuery(qry);
			ResultSetMetaData rsMeta = rs.getMetaData();
			rs.last();
			int rowCount = rs.getRow();
			int colCount = rsMeta.getColumnCount();
			allocatedArray = new String[rowCount][colCount];
			rs.beforeFirst();
			int i = 0;
			while (rs.next()) {
				for (int j = 0; j < colCount; j++)
					allocatedArray[i][j] = rs.getString(j + 1);
				i++;
			}
			if (verbose)
				System.out.println(qry);
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not populate the array with the result set\n"
							+ ex.getMessage());
		}
		return allocatedArray;
	}

	/**
	 * Creates an array of nonindex elements.
	 * 
	 * @param keyIndexes
	 *            a list of the key indexes
	 * @param cols
	 *            the names of the database columns
	 * @return an array that contains non key columns
	 */
	private String[] removeByIndex(ArrayList<Integer> keyIndexes, String[] cols) {
		String[] newArray = new String[cols.length - keyIndexes.size()];
		int j = 0;
		for (int i = 0; i < cols.length; i++) {
			if (!keyIndexes.contains(i)) {
				newArray[j] = cols[i];
				j++;
			}
		}
		return newArray;
	}

	/**
	 * Returns the rows associated with the search query
	 * 
	 * @param qry
	 *            the search query
	 * @param cursor
	 *            the name of the database cursor
	 * @param limit
	 *            the row limit of the database query
	 * @return the database rows retrieved by the query
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String[][] returnSearchRows(String qry, String cursor, int limit)
			throws MobbedException {
		String[][] rows = {};
		try {
			if (!isEmpty(cursor) && limit != Double.POSITIVE_INFINITY) {
				if (!cursorExists(cursor))
					createCursor(cursor, qry);
				rows = fetchFromCursor(cursor, limit);
			} else {
				rows = putRowsInArray(qry);
			}
		} catch (MobbedException ex) {
			throw new MobbedException("Could not return rows\n"
					+ ex.getMessage());
		}
		return rows;
	}

	/**
	 * Set the values of a prepared statement object that inserts rows into the
	 * database.
	 * 
	 * @param smt
	 *            the prepared statement object used to do the queries
	 * @param cols
	 *            the names of the database columns
	 * @param vals
	 *            the values of the non-double database columns
	 * @param dvals
	 *            the values of the double database columns
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void setInsertStatementValues(PreparedStatement smt, String[] cols,
			String[] vals, Double[] dvals) throws MobbedException {
		int numCols = cols.length;
		int i = 0;
		int j = 0;
		try {
			for (int k = 0; k < numCols; k++) {
				int jdbcType = getJDBCType(cols[k]);
				if (!isEmpty(dvals) && jdbcType == Types.DOUBLE) {
					if (dvals[i] != null)
						smt.setDouble(k + 1, dvals[i]);
					else
						smt.setObject(k + 1, dvals[i]);
					i++;
				} else {
					smt.setObject(k + 1, vals[j], jdbcType);
					j++;
				}
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not set value\n" + ex.getMessage());
		}
	}

	/**
	 * Set the values of a prepared statement object that retrieves rows from
	 * the database.
	 * 
	 * @param smt
	 *            the prepared statement object used to do the query
	 * @param cols
	 *            the names of the columns
	 * @param vals
	 *            the values of the columns
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void setSelectStatementValues(PreparedStatement smt, String[] cols,
			String[] vals) throws MobbedException {
		for (int i = 0; i < vals.length; i++) {
			int jdbcType = getJDBCType(cols[i]);
			try {
				smt.setObject(i + 1, vals[i], jdbcType);
			} catch (SQLException ex) {
				throw new MobbedException("Could not set value\n"
						+ ex.getMessage());
			}
		}
	}

	/**
	 * Set the values of a prepared statement object that updates rows in the
	 * database.
	 * 
	 * @param keyIndexes
	 *            a list of the key indexes
	 * @param pstmt
	 *            the prepared statement object used to do the query
	 * @param columns
	 *            the names of the database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void setUpdateStatementValues(ArrayList<Integer> keyIndexes,
			PreparedStatement pstmt, String[] columns, String[] values,
			Double[] doubleValues) throws MobbedException {
		String[] keyColumns = addByIndex(keyIndexes, columns);
		String[] nonKeyColumns = removeByIndex(keyIndexes, columns);
		String[] keyValues = addByIndex(keyIndexes, values);
		String[] nonKeyValues = removeByIndex(keyIndexes, values);
		int i;
		int k = 0;
		int l = 0;
		try {
			for (i = 0; i < nonKeyColumns.length; i++) {
				int targetType = getJDBCType(nonKeyColumns[i]);
				if (doubleValues != null && targetType == Types.DOUBLE) {
					if (doubleValues[k] != null)
						pstmt.setDouble(i + 1, doubleValues[k]);
					else
						pstmt.setObject(i + 1, doubleValues[k]);
					k++;
				} else {
					pstmt.setObject(i + 1, nonKeyValues[l], targetType);
					l++;
				}
			}
			for (int j = 0; j < keyColumns.length; j++) {
				int targetType = getJDBCType(keyColumns[j]);
				pstmt.setObject(i + j + 1, keyValues[j], targetType);
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not set value\n" + ex.getMessage());
		}
	}

	/**
	 * Validates the column names of a table in the database.
	 * 
	 * @param columns
	 *            the names of the database columns
	 * @return true if the database columns are valid, throws an exception if
	 *         otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean validateColumns(String[] columns) throws MobbedException {
		if (!isEmpty(columns)) {
			int numColumns = columns.length;
			for (int i = 0; i < numColumns; i++) {
				if (getColumnType(columns[i].toLowerCase()) == null)
					throw new MobbedException("column " + columns[i]
							+ " is an invalid column type");
			}
		}
		return true;
	}

	/**
	 * Validates the table and column names
	 * 
	 * @param table
	 *            the name of the database table
	 * @param columns
	 *            the non-double columns of the database table
	 * @param doubleColumns
	 *            the double columns of the database table
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void validateTableAndColumns(String table, String[] columns,
			String[] doubleColumns) throws MobbedException {
		validateTableName(table);
		validateColumns(columns);
		validateColumns(doubleColumns);
	}

	/**
	 * Validates a database table.
	 * 
	 * @param table
	 *            the name of the database table
	 * @return true if the databse table is valid, throws an exception if
	 *         otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean validateTableName(String table) throws MobbedException {
		if (getColumnNames(table.toLowerCase()) == null)
			throw new MobbedException("table " + table + " is an invalid table");
		return true;
	}

	/**
	 * Validates a value of a database column.
	 * 
	 * @param column
	 *            the name of the database column
	 * @param value
	 *            the value of the database column
	 * @return true if the value of the database column is valid, false if
	 *         otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean validateValues(String column, String value)
			throws MobbedException {
		String type = typeMap.get(column.toLowerCase());
		try {
			if (type.equalsIgnoreCase("uuid"))
				UUID.fromString(value);
			else if (type.equalsIgnoreCase("integer"))
				Integer.parseInt(value);
			else if (type.equalsIgnoreCase("bigint"))
				Long.parseLong(value);
			else if (type.equalsIgnoreCase("timestamp without time zone"))
				Timestamp.valueOf(value);
		} catch (Exception ex) {
			throw new MobbedException("Invalid type, column: " + column
					+ " value: " + value);
		}
		return true;
	}

	/**
	 * Checks for open database connections.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void checkOpenConnections(Connection dbCon)
			throws MobbedException {
		int otherConnections;
		try {
			Statement stmt = dbCon.createStatement();
			String qry = "SELECT count(pid) from pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()";
			ResultSet rs = stmt.executeQuery(qry);
			rs.next();
			otherConnections = rs.getInt(1);
		} catch (SQLException ex1) {
			throw new MobbedException(
					"Could not execute query to get active connections\n"
							+ ex1.getMessage());
		}
		if (otherConnections > 0) {
			try {
				dbCon.close();
			} catch (SQLException ex2) {
				throw new MobbedException("Could not close the connection\n"
						+ ex2.getMessage());
			}
			throw new MobbedException(
					"Close all connections before dropping the database");
		}
	}

	/**
	 * Closes the database connections of the ManageDB objects in the HashMap
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static synchronized void closeAllConnections()
			throws MobbedException {
		Set<ManageDB> keySet = dbMap.keySet();
		ManageDB[] mds = keySet.toArray(new ManageDB[keySet.size()]);
		int numKeys = mds.length;
		for (int i = 0; i < numKeys; i++) {
			mds[i].closeConnection();
		}
	}

	/**
	 * Concatenates two strings with a space in between them
	 * 
	 * @param str1
	 *            string 1
	 * @param str2
	 *            string 2
	 * @return concatenated string
	 */
	public static String concatStrs(String str1, String str2) {
		if (isEmpty(str1) || isEmpty(str2))
			return str1 + str2;
		return str1 + " " + str2;
	}

	/**
	 * Creates and populates a database. The database must not already exist to
	 * create it. The database will be created from a valid SQL file.
	 * 
	 * @param database
	 *            the name of the database
	 * @param hostname
	 *            the host name of the database
	 * @param username
	 *            the user name of the database
	 * @param password
	 *            the password of the database
	 * @param filename
	 *            the name of the sql file
	 * @param verbose
	 *            prints informative messages if true
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void createDatabase(String database, String hostname,
			String username, String password, String filename, boolean verbose)
			throws MobbedException {
		if (isEmpty(filename))
			throw new MobbedException("The SQL file does not exist");
		try {
			Connection templateCon = establishConnection(templateDatabase,
					hostname, username, password);
			createDatabase(templateCon, database);
			templateCon.close();
			Connection databaseCon = establishConnection(database, hostname,
					username, password);
			createTables(databaseCon, filename);
			databaseCon.close();
			if (verbose)
				System.out.println("Database " + database + " created");
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not create and populate the database\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Drops the database and all objects associated with oids. The database
	 * must already exist to delete it. There must be no active connections to
	 * delete the database.
	 * 
	 * @param database
	 *            the name of the database
	 * @param hostname
	 *            the host name of the database
	 * @param username
	 *            the user name of the database
	 * @param password
	 *            the password of the database
	 * @param verbose
	 *            prints informative messages if true
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void dropDatabase(String database, String hostname,
			String username, String password, boolean verbose)
			throws MobbedException {
		try {
			Connection databaseCon = establishConnection(database, hostname,
					username, password);
			checkOpenConnections(databaseCon);
			deleteDatasetOids(databaseCon);
			deleteDataDefOids(databaseCon);
			databaseCon.close();
			Connection templateCon = establishConnection(templateDatabase,
					hostname, username, password);
			dropDatabase(templateCon, database);
			templateCon.close();
		} catch (SQLException ex) {
			throw new MobbedException("Could not delete the database\n"
					+ ex.getMessage());
		}
		if (verbose)
			System.out.println("Database " + database + " dropped");
	}

	/**
	 * Executes a SQL statement. The sql statement must be valid or an exception
	 * will be thrown.
	 * 
	 * @param con
	 *            a connection to the database
	 * @param sql
	 *            the sql statement to be executed
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void executeSQL(Connection con, String sql)
			throws MobbedException {
		try {
			Statement smt = con.createStatement();
			smt.execute(sql);
		} catch (SQLException ex1) {
			try {
				con.close();
			} catch (SQLException ex2) {
				throw new MobbedException(
						"Could not close the database connection\n"
								+ ex2.getMessage());
			}
			throw new MobbedException("Could not execute the sql statement\n"
					+ ex1.getMessage());
		}
	}

	/**
	 * Initializes the tagMap field that maps tag names to tag uuids.
	 * 
	 * @param con
	 *            a connection to the database
	 * @return a HashMap that contains the tags in the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static HashMap<String, UUID> InitializeTagMap(Connection con)
			throws MobbedException {
		String tagQry = "SELECT TAG_NAME, TAG_UUID FROM TAGS";
		HashMap<String, UUID> tagMap = new HashMap<String, UUID>();
		try {
			Statement smt = con.createStatement();
			ResultSet rs = smt.executeQuery(tagQry);
			while (rs.next()) {
				tagMap.put(rs.getString(1).toUpperCase(),
						UUID.fromString(rs.getString(2)));
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve tags\n"
					+ ex.getMessage());
		}
		return tagMap;
	}

	/**
	 * Checks if an array is empty.
	 * 
	 * @param obj
	 *            the array that is checked to be empty
	 * @return true if the array is empty, false if otherwise
	 */
	public static boolean isEmpty(Object[] obj) {
		boolean empty = true;
		if (obj != null) {
			if (obj.length > 0)
				empty = false;
		}
		return empty;
	}

	/**
	 * Checks if an string is empty.
	 * 
	 * @param str
	 *            the string that is checked to be empty
	 * @return true if the string is empty, false if otherwise
	 */
	public static boolean isEmpty(String str) {
		boolean empty = true;
		if (str != null) {
			if (str.length() > 0)
				empty = false;
		}
		return empty;
	}

	/**
	 * Loads the database credentials from a property file. The credentials will
	 * be stored in a array.
	 * 
	 * @param file
	 *            the name of the property file
	 * @return an array that contains the database credentials
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static String[] loadCredentials(String file) throws Exception {
		Properties props = new Properties();
		String[] creds = {};
		try {
			props.load(new FileInputStream(file));
			creds = new String[props.size()];
			creds[0] = props.getProperty("dbname");
			creds[1] = props.getProperty("hostname");
			creds[2] = props.getProperty("username");
			creds[3] = props.getProperty("password");
		} catch (IOException ex) {
			throw new MobbedException(
					"Could not load the database credentials from the property file\n"
							+ ex.getMessage());
		}
		return creds;
	}

	/**
	 * Stores the database credentials in a property file. Call loadCredentials
	 * to get the database credentials back.
	 * 
	 * @param filename
	 *            the filename of the property file
	 * @param database
	 *            the name of the database
	 * @param hostname
	 *            the host name of the database
	 * @param username
	 *            the user name of the database
	 * @param password
	 *            the password of the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void storeCredentials(String filename, String database,
			String hostname, String username, String password)
			throws MobbedException {
		Properties props = new Properties();
		try {
			props.setProperty("dbname", database);
			props.setProperty("hostname", hostname);
			props.setProperty("username", username);
			props.setProperty("password", password);
			props.store(new FileOutputStream(filename), null);
		} catch (IOException ex) {
			throw new MobbedException("Could not create credentials\n"
					+ ex.getMessage());
		}

	}

	/**
	 * Stores tags in a database. Duplicates will be ignored.
	 * 
	 * @param con
	 *            a connection to the database
	 * @param tagMap
	 *            a HashMap that contains the tags from the database
	 * @param entityType
	 *            the type of entity associated with the tags
	 * @param entityUuid
	 *            the entity uuid
	 * @param tags
	 *            the tags associated with the entity
	 * @return an updated HashMap of the tags from the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static HashMap<String, UUID> storeTags(Connection con,
			HashMap<String, UUID> tagMap, String entityType, UUID entityUuid,
			String[] tags) throws MobbedException {
		String tagEntityQry = "INSERT INTO TAG_ENTITIES (TAG_ENTITY_UUID, TAG_ENTITY_TAG_UUID,TAG_ENTITY_CLASS) VALUES (?,?,?)";
		String tagQry = "INSERT INTO TAGS (TAG_UUID, TAG_NAME) VALUES (?,?)";
		try {
			PreparedStatement tagstmt = con.prepareStatement(tagQry);
			PreparedStatement tagEntitystmt = con
					.prepareStatement(tagEntityQry);
			for (int j = 0; j < tags.length; j++) {
				if (!tagMap.containsKey(tags[j].toUpperCase())) {
					UUID tagUuid = UUID.randomUUID();
					tagstmt.setObject(1, tagUuid, Types.OTHER);
					tagstmt.setString(2, tags[j]);
					tagstmt.execute();
					tagMap.put(tags[j].toUpperCase(), tagUuid);
				}
				tagEntitystmt.setObject(1, entityUuid, Types.OTHER);
				tagEntitystmt.setObject(2, tagMap.get(tags[j].toUpperCase()),
						Types.OTHER);
				tagEntitystmt.setString(3, entityType);
				tagEntitystmt.execute();
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add the event tags to the batch\n"
							+ ex.getMessage());
		}
		return tagMap;
	}

	/**
	 * Adds the ManageDB object to a HashMap
	 * 
	 * @param obj
	 *            the ManageDB object
	 */
	private static synchronized void addManageDB(ManageDB obj) {
		dbMap.put(obj, null);
	}

	/**
	 * Creates a database. The database must not already exist to create it. The
	 * database is created without any tables, columns, and data.
	 * 
	 * @param con
	 *            the connection to the database
	 * @param database
	 *            the name of the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void createDatabase(Connection con, String database)
			throws MobbedException {
		String sql = concatStrs("CREATE DATABASE", database);
		try {
			PreparedStatement smt = con.prepareStatement(sql);
			smt.execute();
		} catch (SQLException ex1) {
			try {
				con.close();
			} catch (SQLException ex2) {
				throw new MobbedException(
						"Could not close the database connection\n"
								+ ex2.getMessage());
			}
			throw new MobbedException("Could not create the database "
					+ database + "\n" + ex1.getMessage());
		}
	}

	/**
	 * Creates the database tables and populates them from a valid SQL file.
	 * 
	 * @param con
	 *            a connection to the database
	 * @param sqlFile
	 *            the name of the SQL file used to create the database tables
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void createTables(Connection con, String sqlFile)
			throws MobbedException {
		DataInputStream in;
		byte[] buffer;
		try {
			File file = new File(sqlFile);
			buffer = new byte[(int) file.length()];
			in = new DataInputStream(new FileInputStream(file));
			in.readFully(buffer);
			in.close();
			String result = new String(buffer);
			String[] tables = result.split("-- execute");
			Statement smt = con.createStatement();
			for (int i = 0; i < tables.length; i++)
				smt.execute(tables[i]);
		} catch (Exception ex) {
			throw new MobbedException(
					"Could not populate the database tables\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Deletes the objects associated with the oids in the datadefs table.
	 * 
	 * @param con
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void deleteDataDefOids(Connection con)
			throws MobbedException {
		try {
			String qry = "SELECT DATADEF_OID FROM DATADEFS WHERE DATADEF_OID IS NOT NULL";
			LargeObjectManager lobj = ((org.postgresql.PGConnection) con)
					.getLargeObjectAPI();
			Statement smt = con.createStatement();
			ResultSet rs = smt.executeQuery(qry);
			while (rs.next()) {
				lobj.unlink(rs.getLong(1));
			}
		} catch (SQLException ex1) {
			try {
				con.close();
			} catch (SQLException ex2) {
				throw new MobbedException(
						"Could not close the database connection\n"
								+ ex2.getMessage());
			}
			throw new MobbedException(
					"Could not delete the objects associated with the oids in datadefs table\n"
							+ ex1.getMessage());
		}
	}

	/**
	 * Deletes the objects associated with the oids in the datasets table.
	 * 
	 * @param con
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void deleteDatasetOids(Connection con)
			throws MobbedException {
		try {
			String qry = "SELECT DATASET_OID FROM DATASETS WHERE DATASET_OID IS NOT NULL";
			LargeObjectManager lobj = ((org.postgresql.PGConnection) con)
					.getLargeObjectAPI();
			Statement smt = con.createStatement();
			ResultSet rs = smt.executeQuery(qry);
			while (rs.next())
				lobj.unlink(rs.getLong(1));
		} catch (SQLException ex1) {
			try {
				con.close();
			} catch (SQLException ex2) {
				throw new MobbedException(
						"Could not close the database connection\n"
								+ ex2.getMessage());
			}
			throw new MobbedException(
					"Could not delete the objects associated with the oids in datasets table\n"
							+ ex1.getMessage());
		}
	}

	/**
	 * Drops a database. The database must already exist to drop it. There must
	 * be no active connections to drop the database.
	 * 
	 * @param con
	 *            connection to a database used to drop a separate database
	 * @param database
	 *            the name of the database to drop
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void dropDatabase(Connection con, String database)
			throws MobbedException {
		String sql = "DROP DATABASE IF EXISTS " + database;
		try {
			Statement smt = con.createStatement();
			smt.execute(sql);
		} catch (SQLException ex1) {
			try {
				con.close();
			} catch (SQLException ex2) {
				throw new MobbedException("Could not close the connection\n"
						+ ex2.getMessage());
			}
			throw new MobbedException("Could not drop the database" + database
					+ "\n" + ex1.getMessage());
		}
	}

	/**
	 * Establishes a connection to a database. The database must exist and allow
	 * connections for a connection to be established.
	 * 
	 * @param database
	 *            the name of the database
	 * @param hostname
	 *            the host name of the database
	 * @param username
	 *            the user name of the database
	 * @param password
	 *            the password of the database
	 * @return a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static Connection establishConnection(String database,
			String hostname, String username, String password)
			throws MobbedException {
		Connection con = null;
		String url = "jdbc:postgresql://" + hostname + "/" + database;
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException ex) {
			throw new MobbedException("Class was not found\n" + ex.getMessage());
		}
		try {
			con = DriverManager.getConnection(url, username, password);
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not establish a connection to database " + database
							+ "\n" + ex.getMessage());
		}
		return con;
	}

	/**
	 * Removes the ManageDB object from the HashMap
	 * 
	 * @param md
	 *            the MangeDB object
	 */
	private static synchronized void removeManageDB(ManageDB md) {
		dbMap.remove(md);
	}

}