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
	 * A hashmap that contains the column names of each database table
	 */
	private HashMap<String, String[]> columnMap;
	/**
	 * A connection to the database
	 */
	private Connection connection;
	/**
	 * A hashmap that contains the default column values of each database table
	 */
	private HashMap<String, String> defaultValues;
	/**
	 * A hashmap that contains the keys of each database table
	 */
	private HashMap<String, String[]> keyMap;
	/**
	 * A hashmap that contains the column types of each database table
	 */
	private HashMap<String, String> typeMap;
	/**
	 * prints informative messages if true
	 */
	private boolean verbose;
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
	 * A query that retrieves column metadata of a database table
	 */
	private static final String COLUMN_QUERY = "SELECT column_default, column_name, data_type from information_schema.columns where table_schema = 'public' AND table_name = ?";
	/**
	 * A hashmap that contains instances of ManageDB objects
	 */
	private static HashMap<ManageDB, String> dbMap;
	/**
	 * A query that retrieves the keys of a database table
	 */
	private static final String KEY_QUERY = "SELECT pg_attribute.attname FROM pg_index, pg_class, pg_attribute"
			+ " WHERE pg_class.oid = ?::regclass AND"
			+ " indrelid = pg_class.oid AND"
			+ " pg_attribute.attrelid = pg_class.oid AND"
			+ " pg_attribute.attnum = any(pg_index.indkey)"
			+ " AND indisprimary";
	/**
	 * A query that retrieves the tables of a database
	 */
	private static final String TABLE_QUERY = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
	/**
	 * The name of the template database
	 */
	private static final String TEMPLATE_DATABASE = "template1";

	/**
	 * Creates a ManageDB object.
	 * 
	 * @param dbname
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
	public ManageDB(String dbname, String hostname, String username,
			String password, boolean verbose) throws MobbedException {
		connection = establishConnection(dbname, hostname, username, password);
		this.verbose = verbose;
		setAutoCommit(false);
		initializeHashMaps();
		put(this);
	}

	/**
	 * Inserts or updates rows in the database. To insert rows, do not assign
	 * values to the key columns. To update rows, assign values to the key
	 * columns that already exist in the database.
	 * 
	 * @param table
	 *            the name of the database table
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleColumns
	 *            the names of the double database columns database table
	 * @param doubleValues
	 *            the values of the double database columns
	 * @return the keys of the database rows that were inserted and/or updated
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addRows(String table, String[] columns, String[][] values,
			String[] doubleColumns, Double[][] doubleValues)
			throws MobbedException {
		validateTableName(table);
		validateColumns(columns);
		int numRows = values.length;
		int numValues = values[0].length;
		int doubleIndex = 0;
		Double[] currentDoubleValues;
		String[] keyList = new String[numRows];
		ArrayList<Integer> keyIndexes = findKeyIndexes(table, columns);
		String insertQry = constructInsertQuery(table, columns);
		String updateQry = constructUpdateQuery(keyIndexes, table, columns);
		try {
			PreparedStatement insertStmt = connection
					.prepareStatement(insertQry);
			PreparedStatement updateStmt = connection
					.prepareStatement(updateQry);
			for (int i = 0; i < numRows; i++) {
				for (int j = 0; j < numValues; j++) {
					if (!isEmpty(values[i][j]))
						validateValues(columns[j], values[i][j]);
					else {
						if (!keyIndexes.contains(j)
								&& !columns[j].equals("dataset_session_uuid"))
							values[i][j] = getDefaultValue(columns[j]);
					}
				}
				if (!isEmpty(doubleValues))
					currentDoubleValues = doubleValues[doubleIndex++];
				else
					currentDoubleValues = null;
				if (keysExist(keyIndexes, table, columns, values[i])) {
					setUpdateStatementValues(keyIndexes, updateStmt, columns,
							values[i], currentDoubleValues);
					if (verbose)
						System.out.println(updateStmt);
					updateStmt.addBatch();
				} else {
					values[i] = generateKeys(keyIndexes, table, columns,
							values[i]);
					setInsertStatementValues(insertStmt, columns, values[i],
							currentDoubleValues);
					if (verbose)
						System.out.println(insertStmt);
					insertStmt.addBatch();

				}
				keyList[i] = addKeyValue(keyIndexes, values[i]);
			}
			insertStmt.executeBatch();
			updateStmt.executeBatch();
		} catch (SQLException me) {
			throw new MobbedException(
					"Could not insert or update row(s) in the database\n"
							+ me.getNextException().getMessage());
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
		String query = "SELECT MAX(DATASET_VERSION) AS LATESTVERSION"
				+ " FROM DATASETS WHERE DATASET_NAMESPACE = ? AND DATASET_NAME = ?";
		int version;
		try {
			PreparedStatement selStmt = connection.prepareStatement(query);
			selStmt.setString(1, namespace);
			selStmt.setString(2, name);
			ResultSet rs = selStmt.executeQuery();
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
	public void close() throws MobbedException {
		try {
			connection.close();
			remove(this);
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
		String query = "CLOSE " + name;
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(query);
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
	public void commit() throws MobbedException {
		try {
			connection.commit();
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
			return connection.getAutoCommit();
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
		return columnMap.get(table.toLowerCase());
	}

	/**
	 * Gets the type of a database column.
	 * 
	 * @param column
	 *            the name of the database column
	 * @return the type of the column
	 */
	public String getColumnType(String column) {
		return typeMap.get(column.toLowerCase());
	}

	/**
	 * Gets all of the columns types from a database table.
	 * 
	 * @param table
	 *            the name of the database table
	 * @return the types of the columns in the table
	 */
	public String[] getColumnTypes(String table) {
		String[] columnNames = getColumnNames(table.toLowerCase());
		int numColumns = columnNames.length;
		String[] columnTypes = new String[numColumns];
		for (int i = 0; i < numColumns; i++)
			columnTypes[i] = getColumnType(columnNames[i]);
		return columnTypes;
	}

	/**
	 * Gets a database connection.
	 * 
	 * @return a database connection
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * Gets the default value of a database column.
	 * 
	 * @param column
	 *            the name of the database column
	 * @return the default value of a column
	 */
	public String getDefaultValue(String column) {
		return defaultValues.get(column.toLowerCase());
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
		String[] columns = columnMap.get(table.toLowerCase());
		int numColumns = columns.length;
		for (int i = 0; i < numColumns; i++) {
			if (typeMap.get(columns[i]).equalsIgnoreCase("double precision"))
				al.add(columns[i]);
		}
		String[] doubleColumns = al.toArray(new String[al.size()]);
		return doubleColumns;
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
		Set<String> keySet = columnMap.keySet();
		String[] tables = keySet.toArray(new String[keySet.size()]);
		return tables;
	}

	/**
	 * Retrieves rows from the database based on search criteria
	 * 
	 * @param table
	 *            the name of the database table
	 * @param limit
	 *            the maximum number of rows to retrieve
	 * @param regExp
	 * @param tags
	 *            the tags search criteria
	 * @param attributes
	 *            the attributes search criteria
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleColumns
	 *            the names of the double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @param range
	 *            the range to search by double database columns
	 * @param cursorName
	 *            the name of the data cursor
	 * @return the rows found by the search criteria
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[][] retrieveRows(String table, double limit, String regExp,
			String[][] tags, String[][] attributes, String[] columns,
			String[][] values, String[] doubleColumns, Double[][] doubleValues,
			double[][] range, String cursorName) throws MobbedException {
		validateTableName(table);
		validateColumns(columns);
		validateColumns(doubleColumns);
		String[][] rows = null;
		String qry = "SELECT * FROM " + table;
		qry += constructQualificationQuery(table, regExp, tags, attributes,
				columns, values, doubleColumns, doubleValues, range);
		if (isEmpty(cursorName) && limit != Double.POSITIVE_INFINITY)
			qry += " LIMIT " + (int) limit;
		try {
			PreparedStatement pstmt = connection.prepareStatement(qry,
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			setQaulificationValues(pstmt, qry, tags, attributes, columns,
					values, doubleColumns, doubleValues);
			if (!isEmpty(cursorName) && limit != Double.POSITIVE_INFINITY) {
				if (!dataCursorExists(cursorName))
					createDataCursor(cursorName, pstmt.toString());
				rows = next(cursorName, (int) limit);
			} else {
				if (verbose)
					System.out.println(pstmt);
				rows = populateArray(pstmt.executeQuery());
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not execute query to retrieve rows\n"
							+ ex.getMessage());
		}
		return rows;
	}

	/**
	 * Rollback the current transaction. Auto commit mode needs to be set to
	 * false to create a transaction.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void rollback() throws MobbedException {
		try {
			connection.rollback();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not rollback current transaction\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Sets the auto commit mode of the connection.
	 * 
	 * @param autoCommit
	 *            true to enable autocommit mode, false to disable it
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void setAutoCommit(boolean autoCommit) throws MobbedException {
		try {
			connection.setAutoCommit(autoCommit);
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
	 * @param columns
	 *            the names of the database columns
	 * @return a array that contains the key columns
	 */
	private String[] addByIndex(ArrayList<Integer> keyIndexes, String[] columns) {
		String[] newArray = new String[keyIndexes.size()];
		int j = 0;
		for (int i = 0; i < keyIndexes.size(); i++) {
			if (keyIndexes.contains(i)) {
				newArray[j] = columns[i];
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
	 * @param values
	 *            the values of the database columns
	 * @return a key value
	 */
	private String addKeyValue(ArrayList<Integer> keyIndexes, String[] values) {
		String keyValue = null;
		if (keyIndexes.size() == 1)
			keyValue = values[keyIndexes.get(0)];
		else {
			String concatKeys = values[keyIndexes.get(0)];
			for (int i = 1; i < keyIndexes.size(); i++)
				concatKeys += "," + values[keyIndexes.get(i)];
			keyValue = concatKeys;
		}
		return keyValue;
	}

	/**
	 * Constructs a query for double database columns.
	 * 
	 * @param doubleColumns
	 *            the name of the double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @param range
	 *            the range to search by double database columns
	 * @return a query string
	 */
	private String constructDoubleQuery(String[] doubleColumns,
			Double[][] doubleValues, double[][] range) {
		String query = "";
		String columnName;
		int numColumns = doubleColumns.length;
		int numValues;
		for (int i = 0; i < numColumns; i++) {
			numValues = doubleValues[i].length;
			columnName = doubleColumns[i];
			query += columnName + " BETWEEN ?+" + range[i][0] + " AND " + "?+"
					+ range[i][1];
			for (int j = 1; j < numValues; j++)
				query += " OR " + columnName + " BETWEEN ?+" + range[i][0]
						+ " AND " + "?+" + range[i][1];
			if (i != numColumns - 1)
				query += " AND ";
		}
		return query;
	}

	/**
	 * Constructs a insert query.
	 * 
	 * @param table
	 *            the name of the database table
	 * @param columns
	 *            the names of the database columns
	 * @return a insert query string
	 */
	private String constructInsertQuery(String table, String[] columns) {
		String qry = "INSERT INTO " + table;
		qry += " (" + columns[0];
		for (int i = 1; i < columns.length; i++)
			qry += ", " + columns[i];
		qry += ")";
		qry += " VALUES (?";
		for (int j = 1; j < columns.length; j++)
			qry += ", ?";
		qry += ")";
		return qry;
	}

	/**
	 * Constructs a query for non-double database columns
	 * 
	 * @param regExp
	 *            on if regular expressions are allowed, off if otherwise
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @return a query string
	 */
	private String constructNonDoubleQuery(String regExp, String[] columns,
			String[][] values) {
		String type;
		String columnName;
		String query = "";
		int numColumns = columns.length;
		for (int i = 0; i < numColumns; i++) {
			type = typeMap.get(columns[i]);
			if (type.equalsIgnoreCase("character varying"))
				columnName = " UPPER(" + columns[i] + ")";
			else
				columnName = columns[i];
			int numValues = values[i].length;
			if (type.equalsIgnoreCase("character varying")
					&& regExp.equalsIgnoreCase("on")) {
				query += columnName + " ~* ?";
				for (int j = 1; j < numValues; j++)
					query += " OR " + columnName + " ~* ?";
			} else {
				query += columnName + " IN (?";
				for (int j = 1; j < numValues; j++)
					query += ",?";
				query += ")";
			}
			if (i != numColumns - 1)
				query += " AND ";
		}
		return query;
	}

	/**
	 * Constructs a query based on search criteria.
	 * 
	 * @param table
	 *            the name of the database table
	 * @param regExp
	 *            on if regular expressions are allowed, off if otherwise
	 * @param tags
	 *            the tag search criteria
	 * @param attributes
	 *            the attribute search criteria
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleColumns
	 *            the names of the double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @param range
	 *            the range to search by double database columns
	 * @return a string query
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String constructQualificationQuery(String table, String regExp,
			String[][] tags, String[][] attributes, String[] columns,
			String[][] values, String[] doubleColumns, Double[][] doubleValues,
			double[][] range) throws MobbedException {
		String qry = "";
		String closer = "";
		String[] keys = keyMap.get(table);
		if (tags != null || attributes != null) {
			qry = " WHERE " + keys[0] + " IN (";
			closer = ")";
		}
		if (tags != null)
			qry += constructTagAttributesQuery(regExp, "Tags", tags);
		if (attributes != null) {
			if (tags != null)
				qry += " INTERSECT ";
			qry += constructTagAttributesQuery(regExp, "Attributes", attributes);
		}
		if (!isEmpty(columns) || !isEmpty(doubleColumns)) {
			if (tags != null || attributes != null)
				qry += " INTERSECT SELECT " + keys[0] + " FROM " + table
						+ " WHERE ";
			else
				qry += " WHERE ";
			qry += constructTableQuery(regExp, columns, values, doubleColumns,
					doubleValues, range);
		}
		qry += closer;
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
	 * Constructs a query associated with a table in the database.
	 * 
	 * @param regExp
	 *            on if regular expressions are allowed, off if otherwise
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleColumns
	 *            the names of the double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @param range
	 *            the range to search by double database columns
	 * @return a query string
	 */
	private String constructTableQuery(String regExp, String[] columns,
			String[][] values, String[] doubleColumns, Double[][] doubleValues,
			double[][] range) {
		String query;
		if (!isEmpty(columns) && isEmpty(doubleColumns))
			query = constructNonDoubleQuery(regExp, columns, values);
		else if (isEmpty(columns) && !isEmpty(doubleColumns))
			query = constructDoubleQuery(doubleColumns, doubleValues, range);
		else
			query = constructNonDoubleQuery(regExp, columns, values) + " AND ("
					+ constructDoubleQuery(doubleColumns, doubleValues, range)
					+ " )";
		return query;
	}

	/**
	 * Constructs a query associated with tags and attributes.
	 * 
	 * @param regExp
	 *            on if regular expressions are allowed, off if otherwise
	 * @param qualification
	 *            the type of qualification, tag or attribute
	 * @param values
	 *            the values used in the query
	 * @return a query string
	 */
	private String constructTagAttributesQuery(String regExp,
			String qualification, String[][] values) {
		String selectqry = "";
		String columnName = "";
		if (qualification.equalsIgnoreCase("Tags")) {
			selectqry = " SELECT TAG_ENTITY_UUID FROM TAGS"
					+ " WHERE UPPER(TAG_NAME)";
			columnName = "UPPER(TAG_NAME)";
		} else {
			selectqry = " SELECT ATTRIBUTE_ORGANIZATIONAL_UUID"
					+ " FROM attributes WHERE UPPER(ATTRIBUTE_VALUE)";
			columnName = "UPPER(ATTRIBUTE_VALUE)";
		}
		String qry = selectqry;
		int groups = values.length;
		for (int i = 0; i < groups; i++) {
			int numValues = values[i].length;
			if (regExp.equalsIgnoreCase("on")) {
				qry += " ~* ?";
				for (int j = 1; j < numValues; j++)
					qry += " OR " + columnName + " ~* ?";
			} else {
				qry += " IN (?";
				for (int j = 1; j < numValues; j++)
					qry += ",?";
				qry += ")";
			}
			if (i != groups - 1)
				qry += " INTERSECT " + selectqry;
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
	 * @param columns
	 *            the names of the database columns
	 * @return a update query string
	 */
	private String constructUpdateQuery(ArrayList<Integer> keyIndexes,
			String table, String[] columns) {
		String[] nonKeyColumns = removeByIndex(keyIndexes, columns);
		String[] keyColumns = addByIndex(keyIndexes, columns);
		String qry = "UPDATE " + table + " SET " + nonKeyColumns[0] + " = ?";
		for (int i = 1; i < nonKeyColumns.length; i++)
			qry += " , " + nonKeyColumns[i] + " = ?";
		qry += " WHERE " + keyColumns[0] + " = ?";
		for (int j = 1; j < keyColumns.length; j++)
			qry += " AND " + keyColumns[j] + " = ?";
		return qry;
	}

	/**
	 * Creates a data cursor
	 * 
	 * @param name
	 *            the name of the data cursor
	 * @param query
	 *            the query the data cursor will be bounded to
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void createDataCursor(String name, String query)
			throws MobbedException {
		String cursorQuery = "DECLARE " + name
				+ " SCROLL CURSOR WITH HOLD FOR " + query;
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(cursorQuery);
			if (verbose)
				System.out.println(cursorQuery);
		} catch (SQLException ex) {
			throw new MobbedException("Could not create data cursor\n"
					+ ex.getMessage());
		}

	}

	/**
	 * 
	 * @param name
	 *            the name of the data cursor
	 * @return true if the data cursor exists
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private boolean dataCursorExists(String name) throws MobbedException {
		String query = "SELECT EXISTS (SELECT 1 FROM PG_CURSORS WHERE NAME = ?)";
		boolean cursorExists;
		try {
			PreparedStatement pstmt = connection.prepareStatement(query);
			pstmt.setString(1, name);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			cursorExists = rs.getBoolean(1);
		} catch (SQLException ex) {
			throw new MobbedException("Could not check if data cursor exists\n"
					+ ex.getMessage());
		}
		return cursorExists;
	}

	/**
	 * Finds the index of a particular column.
	 * 
	 * @param columns
	 *            the names of the database columns
	 * @param name
	 *            the name of the database column that the index is searched for
	 * @return the index of the database column name
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private int findIndexOfColumn(String[] columns, String name)
			throws MobbedException {
		int index = 0;
		boolean found = false;
		int numColumns = columns.length;
		for (int i = 0; i < numColumns; i++) {
			if (columns[i].equalsIgnoreCase(name)) {
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
	 * @param columns
	 *            the names of the database columns
	 * @return the indexes of the key columns
	 */
	private ArrayList<Integer> findKeyIndexes(String table, String[] columns) {
		String[] keys = keyMap.get(table);
		ArrayList<Integer> keyIndexes = new ArrayList<Integer>();
		for (int i = 0; i < columns.length; i++) {
			for (int j = 0; j < keys.length; j++)
				if (columns[i].equalsIgnoreCase(keys[j]))
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
	 * @param columns
	 *            the names of the database columns
	 * @param values
	 *            the values of the databse columns
	 * @return keys for each row being inserted
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String[] generateKeys(ArrayList<Integer> keyIndexes, String table,
			String[] columns, String[] values) throws MobbedException {
		if (table.equalsIgnoreCase("datasets")) {
			int sessionIndex = findIndexOfColumn(columns,
					"dataset_session_uuid");
			values[sessionIndex] = UUID.randomUUID().toString();
		}
		int numKeys = keyIndexes.size();
		for (int i = 0; i < numKeys; i++) {
			if (isEmpty(values[keyIndexes.get(i)]))
				values[keyIndexes.get(i)] = UUID.randomUUID().toString();
		}
		return values;
	}

	/**
	 * Initializes the hashmaps that contain column metadata.
	 * 
	 * @param columnStatement
	 *            the prepared statement object used for the queries
	 * @param table
	 *            the name of the database table
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void initializeColumnHashMaps(PreparedStatement columnStatement,
			String table) throws MobbedException {
		ArrayList<String> columnNameList = new ArrayList<String>();
		String columnDefault = null;
		String columnName = null;
		String columnType = null;
		try {
			columnStatement.setString(1, table);
			ResultSet rs = columnStatement.executeQuery();
			while (rs.next()) {
				columnDefault = rs.getString(1);
				if (columnDefault != null)
					columnDefault = columnDefault.split(":")[0].replaceAll("'",
							"");
				columnName = rs.getString(2);
				columnType = rs.getString(3);
				defaultValues.put(columnName, columnDefault);
				typeMap.put(columnName, columnType);
				columnNameList.add(columnName);
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve the columns\n"
					+ ex.getMessage());
		}
		String[] columnNames = columnNameList.toArray(new String[columnNameList
				.size()]);
		columnMap.put(table, columnNames);
	}

	/**
	 * Initializes the hashmaps. The hashmaps contain metadata about columns and
	 * the keys from each table.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void initializeHashMaps() throws MobbedException {
		columnMap = new HashMap<String, String[]>();
		typeMap = new HashMap<String, String>();
		defaultValues = new HashMap<String, String>();
		keyMap = new HashMap<String, String[]>();
		ResultSet rs = null;
		PreparedStatement columnStatement = null;
		PreparedStatement keyStatement = null;
		try {
			Statement tableStatement = connection.createStatement();
			columnStatement = connection.prepareCall(COLUMN_QUERY);
			keyStatement = connection.prepareCall(KEY_QUERY);
			rs = tableStatement.executeQuery(TABLE_QUERY);
			while (rs.next()) {
				initializeColumnHashMaps(columnStatement,
						rs.getString("table_name"));
				initializeKeyHashMap(keyStatement, rs.getString("table_name"));
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not initialize statement objects\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Initializes a hashmap that contains the keys of each table.
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
			throw new MobbedException("Could not initialize key hashmap\n"
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
				PreparedStatement pstmt = connection
						.prepareStatement(selectQuery);
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
	 * Looks up the jdbc sql types of a given column.
	 * 
	 * @param column
	 *            the name of the database column
	 * @return the jdbc sql type of the column
	 */
	private int lookupTargetType(Object column) {
		// 1.6 doesn't support switch statement with string object
		String type = typeMap.get(column);
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
	 * Fetches the next set of rows that a data cursor points to
	 * 
	 * @param name
	 *            the name of the data cursor
	 * @param fetchSize
	 *            the fetch size of the data cursor
	 * @return a set of rows that the data cursor fetches
	 * @throws MobbedException
	 */
	private String[][] next(String name, int fetchSize) throws MobbedException {
		String[][] rows = null;
		String query = "FETCH FORWARD " + fetchSize + " FROM " + name;
		try {
			Statement stmt = connection.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = stmt.executeQuery(query);
			rows = populateArray(rs);
			if (isEmpty(rows))
				closeCursor(name);
			if (verbose)
				System.out.println(query);
		} catch (SQLException ex) {
			throw new MobbedException("Could not fetch the next set of rows\n"
					+ ex.getMessage());
		}
		return rows;
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
	private String[][] populateArray(ResultSet rs) throws MobbedException {
		String[][] allocatedArray = null;
		try {
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
	 * @param columns
	 *            the names of the database columns
	 * @return an array that contains non key columns
	 */
	private String[] removeByIndex(ArrayList<Integer> keyIndexes,
			String[] columns) {
		String[] newArray = new String[columns.length - keyIndexes.size()];
		int j = 0;
		for (int i = 0; i < columns.length; i++) {
			if (!keyIndexes.contains(i)) {
				newArray[j] = columns[i];
				j++;
			}
		}
		return newArray;
	}

	/**
	 * * Sets the double values of a prepared statement object that retrieves
	 * rows from a particular table in the database.
	 * 
	 * @param pstmt
	 *            the prepared statement object used to do the query
	 * @param valueCount
	 *            the number of values that have already been set
	 * @param doubleColumns
	 *            the names of the double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @return
	 * @throws MobbedException
	 */
	private int setDoubleTableStatementValues(PreparedStatement pstmt,
			int valueCount, String[] doubleColumns, Double[][] doubleValues)
			throws MobbedException {
		int numColumns = doubleColumns.length;
		int numValues;
		for (int i = 0; i < numColumns; i++) {
			numValues = doubleValues[i].length;
			for (int j = 0; j < numValues; j++) {
				try {
					pstmt.setDouble(valueCount, doubleValues[i][j]);
					pstmt.setDouble(valueCount + 1, doubleValues[i][j]);
				} catch (SQLException ex) {
					throw new MobbedException("Could not set value in query\n"
							+ ex.getMessage());
				}
				valueCount = valueCount + 2;
			}
		}
		return valueCount;

	}

	/**
	 * Set the values of a prepared statement object that inserts rows into the
	 * database.
	 * 
	 * @param pstmt
	 *            the prepared statement object used to do the queries
	 * @param columns
	 *            the names of the database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void setInsertStatementValues(PreparedStatement pstmt,
			String[] columns, String[] values, Double[] doubleValues)
			throws MobbedException {
		int numColumns = columns.length;
		int i = 0;
		int j = 0;
		try {
			for (int k = 0; k < numColumns; k++) {
				int targetType = lookupTargetType(columns[k]);
				if (!isEmpty(doubleValues) && targetType == Types.DOUBLE) {
					if (doubleValues[i] != null)
						pstmt.setDouble(k + 1, doubleValues[i]);
					else
						pstmt.setObject(k + 1, doubleValues[i]);
					i++;
				} else {
					pstmt.setObject(k + 1, values[j], targetType);
					j++;
				}
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not set value\n" + ex.getMessage());
		}
	}

	/**
	 * Sets the non-double values of a prepared statement object that retrieves
	 * rows from a particular table in the database.
	 * 
	 * @param pstmt
	 *            the prepared statement object used to do the query
	 * @param valueCount
	 *            the number of values that have already been set
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @return the number of values that were set in the query in addition to
	 *         the ones prior
	 * @throws MobbedException
	 *             if an error occurs
	 */

	private int setNonDoubleTableStatementValues(PreparedStatement pstmt,
			int valueCount, String[] columns, String[][] values)
			throws MobbedException {
		int numColumns = columns.length;
		int numValues = 0;
		int targetType = 0;
		for (int i = 0; i < numColumns; i++) {
			numValues = values[i].length;
			for (int j = 0; j < numValues; j++) {
				targetType = lookupTargetType(columns[i]);
				// Case insensitive fix
				if (targetType == Types.VARCHAR)
					values[i][j] = values[i][j].toUpperCase();
				try {
					pstmt.setObject(valueCount, values[i][j], targetType);
				} catch (SQLException ex) {
					throw new MobbedException("Could not set value in query\n"
							+ ex.getMessage());
				}
				valueCount++;
			}
		}
		return valueCount;
	}

	/**
	 * Sets the values of a prepared statement object that retrieves rows from
	 * the database.
	 * 
	 * @param pstmt
	 *            the prepared statement object used to do the query
	 * @param qry
	 *            the query that will be executed
	 * @param tags
	 *            the values of the tags search criteria
	 * @param attributes
	 *            the values of the attributes search criteria
	 * @param columns
	 *            the names of the non-double database columns
	 * @param values
	 *            the values of the non-double database columns
	 * @param doubleColumns
	 *            the names of the double database columns
	 * @param doubleValues
	 *            the values of the double database columns
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void setQaulificationValues(PreparedStatement pstmt, String qry,
			String[][] tags, String[][] attributes, String[] columns,
			String[][] values, String[] doubleColumns, Double[][] doubleValues)
			throws MobbedException {
		int valueCount = 1;
		if (tags != null)
			valueCount = setTagAttributesStatementValues(pstmt, valueCount,
					tags);
		if (attributes != null)
			valueCount = setTagAttributesStatementValues(pstmt, valueCount,
					attributes);
		if (!isEmpty(columns))
			valueCount = setNonDoubleTableStatementValues(pstmt, valueCount,
					columns, values);
		if (!isEmpty(doubleColumns))
			valueCount = setDoubleTableStatementValues(pstmt, valueCount,
					doubleColumns, doubleValues);
	}

	/**
	 * Set the values of a prepared statement object that retrieves rows from
	 * the database.
	 * 
	 * @param pstmt
	 *            the prepared statement object used to do the query
	 * @param columns
	 *            the names of the columns
	 * @param values
	 *            the values of the columns
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void setSelectStatementValues(PreparedStatement pstmt,
			String[] columns, String[] values) throws MobbedException {
		for (int i = 0; i < values.length; i++) {
			int targetType = lookupTargetType(columns[i]);
			try {
				pstmt.setObject(i + 1, values[i], targetType);
			} catch (SQLException ex) {
				throw new MobbedException("Could not set value\n"
						+ ex.getMessage());
			}
		}
	}

	/**
	 * Sets the values of a prepared statement object that retrieves rows from
	 * the attributes or tags table in the database.
	 * 
	 * @param pstmt
	 *            the prepared statement object used to do the query
	 * @param valueCount
	 *            the number of values that have already been set
	 * @param values
	 *            the values of the tags or attributes search criteria
	 * @return the number of total values that have been set
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private int setTagAttributesStatementValues(PreparedStatement pstmt,
			int valueCount, String[][] values) throws MobbedException {
		int numGroups = values.length;
		for (int i = 0; i < numGroups; i++) {
			int numValues = values[i].length;
			for (int j = 0; j < numValues; j++) {
				values[i][j] = values[i][j].toUpperCase();
				try {
					pstmt.setObject(valueCount, values[i][j], Types.VARCHAR);
				} catch (SQLException ex) {
					throw new MobbedException("Could not set value\n"
							+ ex.getMessage());
				}
				valueCount++;
			}
		}
		return valueCount;
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
				int targetType = lookupTargetType(nonKeyColumns[i]);
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
				int targetType = lookupTargetType(keyColumns[j]);
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
	 * Checks for active connections.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void checkForActiveConnections(Connection dbCon)
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
	 * Closes the database connections of the ManageDB objects in the hashmap
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static synchronized void closeAll() throws MobbedException {
		if (dbMap == null)
			return;
		Set<ManageDB> keySet = dbMap.keySet();
		ManageDB[] mds = keySet.toArray(new ManageDB[keySet.size()]);
		int numKeys = mds.length;
		for (int i = 0; i < numKeys; i++) {
			mds[i].close();
		}
		dbMap = null;
	}

	/**
	 * Stores the database credentials in a property file. Call loadCredentials
	 * to get the database credentials back.
	 * 
	 * @param filename
	 *            the filename of the property file
	 * @param dbname
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
	public static void createCredentials(String filename, String dbname,
			String hostname, String username, String password)
			throws MobbedException {
		Properties prop = new Properties();
		try {
			prop.setProperty("dbname", dbname);
			prop.setProperty("hostname", hostname);
			prop.setProperty("username", username);
			prop.setProperty("password", password);
			prop.store(new FileOutputStream(filename), null);
		} catch (IOException ex) {
			throw new MobbedException("Could not create credentials\n"
					+ ex.getMessage());
		}

	}

	/**
	 * Creates and populates a database. The database must not already exist to
	 * create it. The database will be created from a valid SQL file.
	 * 
	 * @param dbname
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
	public static void createDatabase(String dbname, String hostname,
			String username, String password, String filename, boolean verbose)
			throws MobbedException {
		if (isEmpty(filename))
			throw new MobbedException("The SQL file does not exist");
		try {
			Connection templateConnection = establishConnection(
					TEMPLATE_DATABASE, hostname, username, password);
			createDatabase(templateConnection, dbname);
			templateConnection.close();
			Connection databaseConnection = establishConnection(dbname,
					hostname, username, password);
			populateTables(databaseConnection, filename);
			databaseConnection.close();
			if (verbose)
				System.out.println("Database " + dbname + " created");
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not create and populate the database\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Deletes the database and all objects associated with oids. The database
	 * must already exist to delete it. There must be no active connections to
	 * delete the database.
	 * 
	 * @param dbname
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
	public static void deleteDatabase(String dbname, String hostname,
			String username, String password, boolean verbose)
			throws MobbedException {
		try {
			Connection databaseConnection = establishConnection(dbname,
					hostname, username, password);
			checkForActiveConnections(databaseConnection);
			deleteDatasetOids(databaseConnection);
			deleteDataDefOids(databaseConnection);
			databaseConnection.close();
			Connection templateConnection = establishConnection(
					TEMPLATE_DATABASE, hostname, username, password);
			dropDatabase(templateConnection, dbname);
			templateConnection.close();
		} catch (SQLException ex) {
			throw new MobbedException("Could not delete the database\n"
					+ ex.getMessage());
		}
		if (verbose)
			System.out.println("Database " + dbname + " dropped");
	}

	/**
	 * Executes a SQL statement. The sql statement must be valid or an exception
	 * will be thrown.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param statement
	 *            the sql statement to be executed
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void executeSQL(Connection dbCon, String statement)
			throws MobbedException {
		try {
			Statement stmt = dbCon.createStatement();
			stmt.execute(statement);
		} catch (SQLException ex1) {
			try {
				dbCon.close();
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
	 * Checks if an array is empty.
	 * 
	 * @param o
	 * @return true if the array is empty, false if otherwise
	 */
	public static boolean isEmpty(Object[] o) {
		boolean empty = true;
		if (o != null) {
			if (o.length > 0)
				empty = false;
		}
		return empty;
	}

	/**
	 * Checks if an string is empty.
	 * 
	 * @param s
	 * @return true if the string is empty, false if otherwise
	 */
	public static boolean isEmpty(String s) {
		boolean empty = true;
		if (s != null) {
			if (s.length() > 0)
				empty = false;
		}
		return empty;
	}

	/**
	 * Loads the database credentials from a property file. The credentials will
	 * be stored in a array.
	 * 
	 * @param filename
	 *            the name of the property file
	 * @return an array that contains the database credentials
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static String[] loadCredentials(String filename) throws Exception {
		Properties prop = new Properties();
		String[] credentials = {};
		try {
			prop.load(new FileInputStream(filename));
			credentials = new String[prop.size()];
			credentials[0] = prop.getProperty("dbname");
			credentials[1] = prop.getProperty("hostname");
			credentials[2] = prop.getProperty("username");
			credentials[3] = prop.getProperty("password");
		} catch (IOException ex) {
			throw new MobbedException(
					"Could not load the database credentials from the property file\n"
							+ ex.getMessage());
		}
		return credentials;
	}

	/**
	 * Creates a database. The database must not already exist to create it. The
	 * database is created without any tables, columns, and data.
	 * 
	 * @param dbCon
	 *            the connection to the database
	 * @param dbname
	 *            the name of the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void createDatabase(Connection dbCon, String dbname)
			throws MobbedException {
		String sql = "CREATE DATABASE " + dbname;
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(sql);
			pStmt.execute();
		} catch (SQLException ex1) {
			try {
				dbCon.close();
			} catch (SQLException ex2) {
				throw new MobbedException(
						"Could not close the database connection\n"
								+ ex2.getMessage());
			}
			throw new MobbedException("Could not create the database " + dbname
					+ "\n" + ex1.getMessage());
		}
	}

	/**
	 * Deletes the objects associated with the oids in the datadefs table.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void deleteDataDefOids(Connection dbCon)
			throws MobbedException {
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			String qry = "SELECT DATADEF_OID FROM DATADEFS WHERE DATADEF_OID IS NOT NULL";
			Statement stmt = dbCon.createStatement();
			ResultSet rs = stmt.executeQuery(qry);
			while (rs.next()) {
				lobj.unlink(rs.getLong(1));
			}
		} catch (SQLException ex1) {
			try {
				dbCon.close();
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
	 * @param dbCon
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void deleteDatasetOids(Connection dbCon)
			throws MobbedException {
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			String qry = "SELECT DATASET_OID FROM DATASETS WHERE DATASET_OID IS NOT NULL";
			Statement stmt = dbCon.createStatement();
			ResultSet rs = stmt.executeQuery(qry);
			while (rs.next())
				lobj.unlink(rs.getLong(1));
		} catch (SQLException ex1) {
			try {
				dbCon.close();
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
	 * @param dbCon
	 *            connection to a different database
	 * @param dbname
	 *            the name of the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void dropDatabase(Connection dbCon, String dbname)
			throws MobbedException {
		String sql = "DROP DATABASE IF EXISTS " + dbname;
		try {
			Statement stmt = dbCon.createStatement();
			stmt.execute(sql);
		} catch (SQLException ex1) {
			try {
				dbCon.close();
			} catch (SQLException ex2) {
				throw new MobbedException("Could not close the connection\n"
						+ ex2.getMessage());
			}
			throw new MobbedException("Could not drop the database" + dbname
					+ "\n" + ex1.getMessage());
		}
	}

	/**
	 * Establishes a connection to a database. The database must exist and allow
	 * connections for a connection to be established.
	 * 
	 * @param dbname
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
	private static Connection establishConnection(String dbname,
			String hostname, String username, String password)
			throws MobbedException {
		Connection dbCon = null;
		String url = "jdbc:postgresql://" + hostname + "/" + dbname;
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException ex) {
			throw new MobbedException("Class was not found\n" + ex.getMessage());
		}
		try {
			dbCon = DriverManager.getConnection(url, username, password);
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not establish a connection to database " + dbname
							+ "\n" + ex.getMessage());
		}
		return dbCon;
	}

	/**
	 * Creates the database tables and populates them from a valid SQL file.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param filename
	 *            the name of the SQL file
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void populateTables(Connection dbCon, String filename)
			throws MobbedException {
		DataInputStream in;
		byte[] buffer;
		try {
			File file = new File(filename);
			buffer = new byte[(int) file.length()];
			in = new DataInputStream(new FileInputStream(file));
			in.readFully(buffer);
			in.close();
			String result = new String(buffer);
			String[] tables = result.split("-- execute");
			Statement stmt = dbCon.createStatement();
			for (int i = 0; i < tables.length; i++)
				stmt.execute(tables[i]);
		} catch (Exception ex) {
			throw new MobbedException(
					"Could not populate the database tables\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Puts the ManageDB object in the hashmap
	 * 
	 * @param obj
	 *            the ManageDB object
	 */
	private static synchronized void put(ManageDB obj) {
		if (dbMap == null) {
			dbMap = new HashMap<ManageDB, String>();
		}
		dbMap.put(obj, null);
	}

	/**
	 * Removes the ManageDB object from the hashmap
	 * 
	 * @param obj
	 *            the MangeDB object
	 */
	private static synchronized void remove(ManageDB obj) {
		dbMap.remove(obj);
		if (dbMap.isEmpty()) {
			dbMap = null;
		}
	}

}