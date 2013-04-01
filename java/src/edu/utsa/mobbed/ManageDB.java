package edu.utsa.mobbed;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.Set;
import java.util.UUID;

import org.postgresql.largeobject.LargeObjectManager;

/**
 * Creates or deletes a mobbed database. The table definitions have to be in the
 * mobbed.xml file in the root directory of MobbedDB.
 * 
 * @author Jeremy Cockfield
 * 
 */

public class ManageDB {
	private HashMap<String, String[]> columnMap;
	private Connection connection;
	private HashMap<String, String> defaultValues;
	private HashMap<String, String[]> keyMap;
	private HashMap<String, String> typeMap;
	private static final String columnQuery = "SELECT column_default, column_name, data_type from information_schema.columns where table_schema = 'public' AND table_name = ?";
	private static final String keyQuery = "SELECT pg_attribute.attname FROM pg_index, pg_class, pg_attribute"
			+ " WHERE pg_class.oid = ?::regclass AND"
			+ " indrelid = pg_class.oid AND"
			+ " pg_attribute.attrelid = pg_class.oid AND"
			+ " pg_attribute.attnum = any(pg_index.indkey)"
			+ " AND indisprimary";
	private static final String tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
	private static final String templateName = "template1";

	/**
	 * Creates a ManageDB object to interact with database
	 * 
	 * @param name
	 * @param hostname
	 * @param user
	 * @param password
	 * @throws Exception
	 */
	public ManageDB(String name, String hostname, String user, String password)
			throws Exception {
		connection = establishConnection(name, hostname, user, password);
		setAutoCommit(false);
		initializeHashMaps();
	}

	/**
	 * Inserts rows into a database
	 * 
	 * @param tableName
	 * @param columnNames
	 * @param columnValues
	 * @return
	 * @throws Exception
	 */
	public String[] addRows(String tableName, String[] columnNames,
			String[][] columnValues, String[] doubleColumnNames,
			Double[][] doubleValues) throws Exception {
		// Validate table name
		validateTable(tableName);
		// Validate column names
		validateColumnNames(columnNames);
		int rowCount = columnValues.length;
		int columnCount = columnValues[0].length;
		Double[] currentDoubleValues = null;
		String[] keyList = new String[rowCount];
		// Find key indexes to check if empty and to construct update query
		ArrayList<Integer> keyIndexes = findKeyIndexes(tableName, columnNames);
		// Construct queries and prepared statements for insert/update
		String insertQry = constructInsertQuery(tableName, columnNames);
		String updateQry = constructUpdateQuery(keyIndexes, tableName,
				columnNames);
		PreparedStatement insertStmt = connection.prepareStatement(insertQry);
		PreparedStatement updateStmt = connection.prepareStatement(updateQry);
		// Loop through each row
		for (int i = 0; i < rowCount; i++) {
			// Loop through each column
			for (int j = 0; j < columnCount; j++) {
				if (!isEmpty(columnValues[i][j]))
					// Validate columns
					validateColumnValue(columnNames[j], columnValues[i][j]);
				else
					// Set defaults
					columnValues[i][j] = getDefaultValue(columnNames[j]);
			}
			// Check if keys are empty and if they exist in database
			if (keysExist(keyIndexes, tableName, columnNames, columnValues[i])) {
				if (!isEmpty(doubleColumnNames))
					currentDoubleValues = doubleValues[i];
				setUpdateStatementValues(keyIndexes, updateStmt, columnNames,
						columnValues[i], doubleValues[i]);
				updateStmt.addBatch();
			} else {
				columnValues[i] = generateKeys(keyIndexes, columnValues[i]);
				if (!isEmpty(doubleColumnNames))
					currentDoubleValues = doubleValues[i];
				setInsertStatementValues(insertStmt, columnNames,
						columnValues[i], currentDoubleValues);
				insertStmt.addBatch();
			}
			// Get keys from row
			keyList[i] = addKeyValue(keyIndexes, columnValues[i]);
		}
		// execute batches
		insertStmt.executeBatch();
		updateStmt.executeBatch();
		System.out.println(insertStmt);
		return keyList;
	}

	/**
	 * Closes a database connection
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception {
		try {
			connection.close();
		} catch (Exception me) {
			throw new MobbedException("Could not close connection\n"
					+ me.getMessage());
		}
	}

	/**
	 * Commits all database transactions
	 * 
	 * @throws Exception
	 */
	public void commit() throws Exception {
		try {
			connection.commit();
		} catch (Exception me) {
			throw new MobbedException("Could not commit transaction(s)\n"
					+ me.getMessage());
		}
	}

	public String[][] extractRows(String inTableName, String[] inColumnNames,
			String[][] inColumnValues, String outTableName,
			String[] outColumnNames, String[][] outColumnValues, int limit,
			String regExp, double lower, double upper) throws Exception {
		String qry = "SELECT * FROM extractRange(?,?,?,?) as (";
		String[] columns = getColumnNames(inTableName);
		for (int i = 0; i < columns.length; i++)
			qry += columns[i] + " " + typeMap.get(columns[i]) + ",";
		qry += "extracted uuid[]) ORDER BY EVENT_ENTITY_UUID, EVENT_START_TIME";
		if (limit > 0)
			qry += " LIMIT " + limit;
		String inQry = "SELECT * FROM " + inTableName;
		if (inColumnNames != null) {
			inQry += constructQualificationQuery(inTableName, regExp, null,
					null, inColumnNames, inColumnValues);
			PreparedStatement inStmt = connection.prepareStatement(inQry);
			setStructureStatementValues(inStmt, 1, inColumnNames,
					inColumnValues);
			inQry = inStmt.toString();
		}
		String outQry = "SELECT * FROM " + inTableName;
		if (outColumnNames != null) {
			outQry += constructQualificationQuery(outTableName, regExp, null,
					null, outColumnNames, outColumnValues);
			PreparedStatement outStmt = connection.prepareStatement(outQry);
			setStructureStatementValues(outStmt, 1, outColumnNames,
					outColumnValues);
			outQry = outStmt.toString();
		}
		PreparedStatement pstmt = connection.prepareStatement(qry,
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		pstmt.setString(1, inQry);
		pstmt.setString(2, outQry);
		pstmt.setDouble(3, lower);
		pstmt.setDouble(4, upper);
		System.out.println(pstmt);
		ResultSet rs = pstmt.executeQuery();
		String[][] rows = populateArray(rs);
		return rows;
	}

	public String[][] extractUniqueRows(String[][] extractedRows, int limit)
			throws Exception {
		int extractedColumn = extractedRows[0].length - 1;
		int numRows = extractedRows.length;
		String[] extractedUuids = null;
		ArrayList<String> alist = new ArrayList<String>();
		for (int i = 0; i < numRows; i++) {
			extractedUuids = extractedRows[i][extractedColumn].replaceAll(
					"[{}]", "").split(",");
			for (int j = 0; j < extractedUuids.length; j++) {
				if (!alist.contains(extractedUuids[j]))
					alist.add("'" + extractedUuids[j] + "'");
			}
		}
		String list = alist.toString().replaceAll("[\\[\\]]", "");
		String qry = "SELECT * FROM EVENTS WHERE EVENT_UUID IN (" + list + ")";
		if (limit > 0)
			qry += " LIMIT " + limit;
		Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery(qry);
		String[][] rows = populateArray(rs);
		return rows;
	}

	/**
	 * Gets the column names of a given table
	 * 
	 * @param table
	 * @return
	 */
	public String[] getColumnNames(String table) {
		return columnMap.get(table);
	}

	/**
	 * Gets the column type given a column name
	 * 
	 * @param columnName
	 * @return
	 */
	public String getColumnType(String columnName) {
		return typeMap.get(columnName);
	}

	public String[] getColumnTypes(String table) {
		String[] columnNames = getColumnNames(table);
		int numColumns = columnNames.length;
		String[] columnTypes = new String[numColumns];
		for (int i = 0; i < numColumns; i++)
			columnTypes[i] = getColumnType(columnNames[i]);
		return columnTypes;
	}

	/**
	 * Gets a database connection
	 * 
	 * @return
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * Gets the default value given a column name
	 * 
	 * @param columnName
	 * @return
	 */
	public String getDefaultValue(String columnName) {
		return defaultValues.get(columnName);
	}

	/**
	 * Gets the columns that are double precision
	 * 
	 * @param tableName
	 * @return
	 */
	public String[] getDoubleColumns(String tableName) {
		ArrayList<String> al = new ArrayList<String>();
		String[] columns = columnMap.get(tableName);
		int numColumns = columns.length;
		for (int i = 0; i < numColumns; i++) {
			if (typeMap.get(columns[i]).equalsIgnoreCase("double precision"))
				al.add(columns[i]);
		}
		String[] doubleColumns = al.toArray(new String[al.size()]);
		return doubleColumns;
	}

	/**
	 * Gets the keys of a given table
	 * 
	 * @param table
	 * @return
	 */
	public String[] getKeys(String table) {
		return keyMap.get(table);
	}

	/**
	 * Gets the tables from a database
	 * 
	 * @return
	 */
	public String[] getTables() {
		Set<String> keySet = columnMap.keySet();
		String[] tables = keySet.toArray(new String[keySet.size()]);
		return tables;
	}

	/**
	 * Retrieves rows from a table given search criteria
	 * 
	 * @param tableName
	 * @param limit
	 * @param regExp
	 * @param tags
	 * @param attributes
	 * @param columnNames
	 * @param columnValues
	 * @return
	 * @throws Exception
	 */
	public String[][] retrieveRows(String tableName, int limit, String regExp,
			String[][] tags, String[][] attributes, String[] columnNames,
			String[][] columnValues) throws Exception {
		validateTable(tableName);
		ResultSet rs = null;
		String qry = "SELECT * FROM " + tableName;
		// Qualifications provided
		if (tags != null || attributes != null || columnNames != null) {
			qry += constructQualificationQuery(tableName, regExp, tags,
					attributes, columnNames, columnValues);
			if (limit > 0)
				qry += " LIMIT " + limit;
			PreparedStatement pstmt = connection.prepareStatement(qry,
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			setQaulificationValues(pstmt, qry, tags, attributes, columnNames,
					columnValues);
			rs = pstmt.executeQuery();
		}
		// Qualifications not provided
		else {
			if (limit > 0)
				qry += " LIMIT " + limit;
			Statement stmt = connection.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(qry);
		}
		String[][] rows = populateArray(rs);
		return rows;
	}

	/**
	 * Rollback all transactions
	 * 
	 * @throws Exception
	 */
	public void rollback() throws Exception {
		try {
			connection.rollback();
		} catch (Exception me) {
			throw new MobbedException("Could not rollback transactions\n"
					+ me.getMessage());
		}
	}

	/**
	 * Sets the auto commit mode
	 * 
	 * @param autoCommit
	 * @throws Exception
	 */
	public void setAutoCommit(boolean autoCommit) throws Exception {
		try {
			connection.setAutoCommit(autoCommit);
		} catch (Exception me) {
			throw new MobbedException("Could not set auto commit mode\n"
					+ me.getMessage());
		}
	}

	/**
	 * Adds elements to String array by index
	 * 
	 * @param keyIndexes
	 * @param array
	 * @return
	 */
	private String[] addByIndex(ArrayList<Integer> keyIndexes, String[] array) {
		// Used to get key column names and values
		String[] newArray = new String[keyIndexes.size()];
		int j = 0;
		for (int i = 0; i < keyIndexes.size(); i++) {
			if (keyIndexes.contains(i)) {
				newArray[j] = array[i];
				j++;
			}
		}
		return newArray;
	}

	/**
	 * Adds a key value to array
	 * 
	 * @param keyIndexes
	 * @param columnValues
	 * @return
	 */
	private String addKeyValue(ArrayList<Integer> keyIndexes,
			String[] columnValues) {
		String keyValue = null;
		if (keyIndexes.size() == 1)
			keyValue = columnValues[keyIndexes.get(0)];
		else {
			String concatKeys = columnValues[keyIndexes.get(0)];
			for (int i = 1; i < keyIndexes.size(); i++)
				concatKeys += "," + columnValues[keyIndexes.get(i)];
			keyValue = concatKeys;
		}
		return keyValue;
	}

	/**
	 * Constructs a insert query
	 * 
	 * @param tableName
	 * @param columnNames
	 * @return
	 */
	private String constructInsertQuery(String tableName, String[] columnNames) {
		String qry = "INSERT INTO " + tableName;
		qry += " (" + columnNames[0];
		for (int i = 1; i < columnNames.length; i++)
			qry += ", " + columnNames[i];
		qry += ")";
		qry += " VALUES (?";
		for (int j = 1; j < columnNames.length; j++)
			qry += ", ?";
		qry += ")";
		return qry;
	}

	/**
	 * Constructs a query based on the qualifications the user provides
	 * 
	 * @param tableName
	 * @param limit
	 * @param regExp
	 * @param tags
	 * @param attributes
	 * @param columnNames
	 * @param columnValues
	 * @return
	 * @throws Exception
	 */
	private String constructQualificationQuery(String tableName, String regExp,
			String[][] tags, String[][] attributes, String[] columnNames,
			String[][] columnValues) throws Exception {
		String[] keys = keyMap.get(tableName);
		String qry = " WHERE " + keys[0] + " IN (";
		if (tags != null)
			qry += constructTagAttributesQuery(regExp, "Tags", tags);
		if (attributes != null) {
			if (tags != null)
				qry += " INTERSECT ";
			qry += constructTagAttributesQuery(regExp, "Attributes", attributes);
		}
		if (columnNames != null) {
			if (tags != null || attributes != null)
				qry += " INTERSECT ";
			qry += constructStructQuery(regExp, tableName, columnNames,
					columnValues);
		}
		qry += ")";
		return qry;
	}

	/**
	 * Constructs a select query
	 * 
	 * @param keyIndexes
	 * @param tableName
	 * @param columnNames
	 * @return
	 */
	private String constructSelectQuery(ArrayList<Integer> keyIndexes,
			String tableName, String[] columnNames) {
		String[] keyColumns = addByIndex(keyIndexes, columnNames);
		String qry = "SELECT * FROM " + tableName + " WHERE " + keyColumns[0]
				+ " = ?";
		for (int i = 1; i < keyColumns.length; i++) {
			qry += " AND " + keyColumns[i] + " = ?";
		}
		return qry;
	}

	/**
	 * Constructs a query from a structure
	 * 
	 * @param regExp
	 * @param tableName
	 * @param columnNames
	 * @param columnValues
	 * @return
	 * @throws Exception
	 */
	private String constructStructQuery(String regExp, String tableName,
			String[] columnNames, String[][] columnValues) throws Exception {
		String[] keys = keyMap.get(tableName);
		String type = typeMap.get(columnNames[0]);
		String columnName = null;
		String qry = " SELECT " + keys[0] + " FROM " + tableName + " WHERE ";
		int numColumns = columnNames.length;
		for (int i = 0; i < numColumns; i++) {
			// Case insensitive fix
			if (type.equalsIgnoreCase("character varying"))
				columnName = "UPPER(" + columnNames[i] + ")";
			else
				columnName = columnNames[i];
			int numValues = columnValues[i].length;
			// With regexp (only works for strings)
			if (type.equalsIgnoreCase("character varying")
					&& regExp.equalsIgnoreCase("on")) {
				qry += columnName + " ~* ?";
				for (int j = 1; j < numValues; j++)
					qry += " OR " + columnName + " ~* ?";
			}
			// Without regexp (works for everything)
			else {
				qry += columnName + " IN (?";
				for (int j = 1; j < numValues; j++)
					qry += ",?";
				qry += ")";
			}
			if (i != numColumns - 1)
				qry += " AND ";
		}
		return qry;
	}

	/**
	 * Constructs a tag or attribute query
	 * 
	 * @param regExp
	 * @param qualification
	 * @param values
	 * @return
	 * @throws Exception
	 */
	private String constructTagAttributesQuery(String regExp,
			String qualification, String[][] values) throws Exception {
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
			}
			// Without regexp (works for everything)
			else {
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
	 * Constructs a update query
	 * 
	 * @param keyIndexes
	 * @param tableName
	 * @param columnNames
	 * @return
	 */
	private String constructUpdateQuery(ArrayList<Integer> keyIndexes,
			String tableName, String[] columnNames) {
		String[] nonKeyColumns = removeByIndex(keyIndexes, columnNames);
		String[] keyColumns = addByIndex(keyIndexes, columnNames);
		String qry = "UPDATE " + tableName + " SET " + nonKeyColumns[0]
				+ " = ?";
		for (int i = 1; i < nonKeyColumns.length; i++)
			qry += " , " + nonKeyColumns[i] + " = ?";
		qry += " WHERE " + keyColumns[0] + " = ?";
		for (int j = 1; j < keyColumns.length; j++)
			qry += " AND " + keyColumns[j] + " = ?";
		return qry;
	}

	/**
	 * Finds the indexes that contain key columns
	 * 
	 * @param tableName
	 * @param columnNames
	 * @return
	 */
	private ArrayList<Integer> findKeyIndexes(String tableName,
			String[] columnNames) {
		String[] keys = keyMap.get(tableName);
		ArrayList<Integer> keyIndexes = new ArrayList<Integer>();
		for (int i = 0; i < columnNames.length; i++) {
			for (int j = 0; j < keys.length; j++)
				if (columnNames[i].equalsIgnoreCase(keys[j]))
					keyIndexes.add(i);
		}
		return keyIndexes;
	}

	/**
	 * Generates key column values
	 * 
	 * @param keyIndexes
	 * @param columnValues
	 * @return
	 */
	private String[] generateKeys(ArrayList<Integer> keyIndexes,
			String[] columnValues) {
		for (int i = 0; i < keyIndexes.size(); i++) {
			// Check if empty because keys could be supplied but don't exist
			if (isEmpty(columnValues[keyIndexes.get(i)]))
				columnValues[keyIndexes.get(i)] = UUID.randomUUID().toString();
		}
		return columnValues;
	}

	/**
	 * Initializes hash maps that involve column names
	 * 
	 * @param columnStatement
	 * @param tableName
	 * @throws Exception
	 */
	private void initializeColumns(PreparedStatement columnStatement,
			String tableName) throws Exception {
		columnStatement.setString(1, tableName);
		ResultSet rs = columnStatement.executeQuery();
		ArrayList<String> columnNameList = new ArrayList<String>();
		String columnDefault = null;
		String columnName = null;
		String columnType = null;
		while (rs.next()) {
			columnDefault = rs.getString(1);
			if (columnDefault != null)
				columnDefault = columnDefault.split(":")[0].replaceAll("'", "");
			columnName = rs.getString(2);
			columnType = rs.getString(3);
			defaultValues.put(columnName, columnDefault);
			typeMap.put(columnName, columnType);
			columnNameList.add(columnName);
		}
		String[] columnNames = columnNameList.toArray(new String[columnNameList
				.size()]);
		columnMap.put(tableName, columnNames);
	}

	/**
	 * Initializes hash map fields
	 * 
	 * @throws Exception
	 */
	private void initializeHashMaps() throws Exception {
		columnMap = new HashMap<String, String[]>();
		typeMap = new HashMap<String, String>();
		defaultValues = new HashMap<String, String>();
		keyMap = new HashMap<String, String[]>();
		Statement tableStatement = connection.createStatement();
		PreparedStatement columnStatement = connection.prepareCall(columnQuery);
		PreparedStatement keyStatement = connection.prepareCall(keyQuery);
		ResultSet rs = tableStatement.executeQuery(tableQuery);
		while (rs.next()) {
			initializeColumns(columnStatement, rs.getString("table_name"));
			initializeKeys(keyStatement, rs.getString("table_name"));
		}
	}

	/**
	 * Initialize hash map for key columns
	 * 
	 * @param keyStatement
	 * @param tableName
	 * @throws Exception
	 */
	private void initializeKeys(PreparedStatement keyStatement, String tableName)
			throws Exception {
		keyStatement.setString(1, tableName);
		ResultSet rs = keyStatement.executeQuery();
		ArrayList<String> keyColumnList = new ArrayList<String>();
		while (rs.next()) {
			keyColumnList.add(rs.getString(1));
		}
		String[] keyColumns = keyColumnList.toArray(new String[keyColumnList
				.size()]);
		keyMap.put(tableName, keyColumns);
	}

	/**
	 * Checks if key columns are empty
	 * 
	 * @param keyIndexes
	 * @param columnValues
	 * @return
	 * @throws Exception
	 */
	private boolean keysEmpty(ArrayList<Integer> keyIndexes,
			String[] columnValues) throws Exception {
		boolean empty = true;
		int keyCount = 0;
		for (int i = 0; i < keyIndexes.size(); i++) {
			if (!isEmpty(columnValues[keyIndexes.get(i)]))
				keyCount++;
		}
		// Composite key insert/update check
		if (keyCount > 0 && keyIndexes.size() > keyCount)
			throw new MobbedException("Composite key is missing column(s)");
		else if (keyCount == keyIndexes.size())
			empty = false;
		return empty;
	}

	/**
	 * Checks if the given keys exist in the database
	 * 
	 * @param keyIndexes
	 * @param tableName
	 * @param columnNames
	 * @param columnValues
	 * @return
	 * @throws Exception
	 */
	private boolean keysExist(ArrayList<Integer> keyIndexes, String tableName,
			String[] columnNames, String[] columnValues) throws Exception {
		boolean exist = false;
		if (!keysEmpty(keyIndexes, columnValues)) {
			String[] keyColumns = addByIndex(keyIndexes, columnNames);
			String[] keyValues = addByIndex(keyIndexes, columnValues);
			String selectQuery = constructSelectQuery(keyIndexes, tableName,
					columnNames);
			PreparedStatement pstmt = connection.prepareStatement(selectQuery);
			setSelectStatementValues(pstmt, keyColumns, keyValues);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next())
				exist = true;
		}
		return exist;
	}

	/**
	 * Looks up the jdbc sql types
	 * 
	 * @param columnName
	 * @return
	 */
	private int lookupTargetType(Object columnName) {
		// 1.6 doesn't support switch statement with string object
		String type = typeMap.get(columnName);
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
	 * Populates an array from the results of a result set
	 * 
	 * @param rs
	 * @return
	 * @throws Exception
	 */
	private String[][] populateArray(ResultSet rs) throws Exception {
		ResultSetMetaData rsMeta = rs.getMetaData();
		rs.last();
		int rowCount = rs.getRow();
		int colCount = rsMeta.getColumnCount();
		String[][] allocatedArray = new String[rowCount][colCount];
		rs.beforeFirst();
		for (int i = 0; i < rowCount; i++) {
			rs.next();
			for (int j = 0; j < colCount; j++)
				allocatedArray[i][j] = rs.getString(j + 1);
		}
		return allocatedArray;
	}

	/**
	 * Creates an array of non-index elements
	 * 
	 * @param keyIndexes
	 * @param array
	 * @return
	 */
	private String[] removeByIndex(ArrayList<Integer> keyIndexes, String[] array) {
		// Used to get non-key column names and values
		String[] newArray = new String[array.length - keyIndexes.size()];
		int j = 0;
		for (int i = 0; i < array.length; i++) {
			if (!keyIndexes.contains(i)) {
				newArray[j] = array[i];
				j++;
			}
		}
		return newArray;
	}

	/**
	 * Set values for insert prepared statement
	 * 
	 * @param pstmt
	 * @param columnNames
	 * @param columnValues
	 * @throws SQLException
	 */
	private void setInsertStatementValues(PreparedStatement pstmt,
			String[] columnNames, String[] columnValues, Double[] doubleValues)
			throws SQLException {
		int numColumns = columnNames.length;
		int i = 0;
		int j = 0;
		for (int k = 0; k < numColumns; k++) {
			int targetType = lookupTargetType(columnNames[k]);
			if (doubleValues != null && targetType == Types.DOUBLE) {
				pstmt.setDouble(k + 1, doubleValues[i]);
				i++;
			} else {
				pstmt.setObject(k + 1, columnValues[j], targetType);
				j++;
			}
		}
	}

	/**
	 * Sets the values of a query constructed by the qualifications
	 * 
	 * @param pstmt
	 * @param qry
	 * @param tags
	 * @param attributes
	 * @param columnNames
	 * @param columnValues
	 * @throws Exception
	 */
	private void setQaulificationValues(PreparedStatement pstmt, String qry,
			String[][] tags, String[][] attributes, String[] columnNames,
			String[][] columnValues) throws Exception {
		int valueCount = 1;
		if (tags != null)
			valueCount = setTagAttributesStatementValues(pstmt, valueCount,
					tags);
		if (attributes != null)
			valueCount = setTagAttributesStatementValues(pstmt, valueCount,
					attributes);
		if (columnNames != null)
			valueCount = setStructureStatementValues(pstmt, valueCount,
					columnNames, columnValues);
	}

	/**
	 * Set values for select prepared statement
	 * 
	 * @param pstmt
	 * @param columnNames
	 * @param columnValues
	 * @throws SQLException
	 */
	private void setSelectStatementValues(PreparedStatement pstmt,
			String[] columnNames, String[] columnValues) throws SQLException {
		for (int i = 0; i < columnValues.length; i++) {
			int targetType = lookupTargetType(columnNames[i]);
			pstmt.setObject(i + 1, columnValues[i], targetType);
		}
	}

	/**
	 * Sets structure query values
	 * 
	 * @param pstmt
	 * @param valueCount
	 * @param columnNames
	 * @param columnValues
	 * @return
	 * @throws SQLException
	 */
	private int setStructureStatementValues(PreparedStatement pstmt,
			int valueCount, String[] columnNames, String[][] columnValues)
			throws SQLException {
		int numColumns = columnNames.length;
		int numValues = 0;
		int targetType = 0;
		for (int i = 0; i < numColumns; i++) {
			numValues = columnValues[i].length;
			for (int j = 0; j < numValues; j++) {
				targetType = lookupTargetType(columnNames[i]);
				// Case insensitive fix
				if (targetType == Types.VARCHAR)
					columnValues[i][j] = columnValues[i][j].toUpperCase();
				pstmt.setObject(valueCount, columnValues[i][j], targetType);
				valueCount++;
			}
		}
		return valueCount;
	}

	/**
	 * Sets tag or attribute query values
	 * 
	 * @param pstmt
	 * @param valueCount
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	private int setTagAttributesStatementValues(PreparedStatement pstmt,
			int valueCount, String[][] values) throws SQLException {
		int numGroups = values.length;
		for (int i = 0; i < numGroups; i++) {
			int numValues = values[i].length;
			for (int j = 0; j < numValues; j++) {
				values[i][j] = values[i][j].toUpperCase();
				pstmt.setObject(valueCount, values[i][j], Types.VARCHAR);
				valueCount++;
			}
		}
		return valueCount;
	}

	/**
	 * Set values for update prepared statement
	 * 
	 * @param keyIndexes
	 * @param pstmt
	 * @param columnNames
	 * @param columnValues
	 * @throws SQLException
	 */
	private void setUpdateStatementValues(ArrayList<Integer> keyIndexes,
			PreparedStatement pstmt, String[] columnNames,
			String[] columnValues, Double[] doubleValues) throws SQLException {
		String[] keyColumns = addByIndex(keyIndexes, columnNames);
		String[] nonKeyColumns = removeByIndex(keyIndexes, columnNames);
		String[] keyValues = addByIndex(keyIndexes, columnValues);
		String[] nonKeyValues = removeByIndex(keyIndexes, columnValues);
		int i;
		int k = 0;
		for (i = 0; i < nonKeyColumns.length; i++) {
			int targetType = lookupTargetType(nonKeyColumns[i]);
			if (doubleValues != null && targetType == Types.DOUBLE) {
				pstmt.setDouble(i + 1, doubleValues[k]);
				k++;
			}
			pstmt.setObject(i + 1, nonKeyValues[i], targetType);
		}
		for (int j = 0; j < keyColumns.length; j++) {
			int targetType = lookupTargetType(keyColumns[j]);
			pstmt.setObject(i + j + 1, keyValues[j], targetType);
		}
	}

	/**
	 * Validates the column names
	 * 
	 * @param columnNames
	 * @return
	 * @throws Exception
	 */
	private boolean validateColumnNames(String[] columnNames) throws Exception {
		int numColumns = columnNames.length;
		for (int i = 0; i < numColumns; i++) {
			if (getColumnType(columnNames[i]) == null)
				throw new MobbedException("column " + columnNames[i]
						+ " is an invalid column type");
		}
		return true;
	}

	/**
	 * Validates a given column value
	 * 
	 * @param columnName
	 * @param columnValue
	 * @return
	 * @throws Exception
	 */
	private boolean validateColumnValue(String columnName, String columnValue)
			throws Exception {
		String type = typeMap.get(columnName);
		try {
			if (type.equalsIgnoreCase("uuid")) {
				UUID.fromString(columnValue);
			} else if (type.equalsIgnoreCase("integer")) {
				Integer.parseInt(columnValue);
			} else if (type.equalsIgnoreCase("bigint")) {
				Long.parseLong(columnValue);
			} else if (type.equalsIgnoreCase("timestamp without time zone")) {
				Timestamp.valueOf(columnValue);
			}
		} catch (Exception me) {
			throw new MobbedException("Invalid type, column: " + columnName
					+ " value: " + columnValue);
		}
		return true;
	}

	/**
	 * Validates a table name
	 * 
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	private boolean validateTable(String tableName) throws Exception {
		if (getColumnNames(tableName) == null)
			throw new MobbedException("table " + tableName
					+ " is an invalid table");
		return true;
	}

	public static void checkForOtherConnections(Connection dbCon)
			throws Exception {
		Statement stmt = dbCon.createStatement();
		String qry = "SELECT count(pid) from pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()";
		ResultSet rs = stmt.executeQuery(qry);
		rs.next();
		int otherConnections = rs.getInt(1);
		if (otherConnections > 0) {
			dbCon.close();
			throw new MobbedException(
					"Close all connections before dropping the database");
		}
	}

	/**
	 * Creates and sets up database
	 * 
	 * @param name
	 * @param hostname
	 * @param user
	 * @param password
	 * @param tablePath
	 * @param verbose
	 * @throws Exception
	 */
	public static void createDatabase(String name, String hostname,
			String user, String password, String tablePath, boolean verbose)
			throws Exception {
		if (isEmpty(tablePath))
			throw new MobbedException("SQL file does not exist");
		Connection templateConnection = establishConnection(templateName,
				hostname, user, password);
		createDatabase(templateConnection, name);
		templateConnection.close();
		Connection databaseConnection = establishConnection(name, hostname,
				user, password);
		createTables(databaseConnection, tablePath);
		executeSQL(databaseConnection, MobbedConstants.USER_DEFINED_FUNCTIONS);
		databaseConnection.close();
		if (verbose)
			System.out.println("Database " + name + " created");
	}

	/**
	 * Deletes a database
	 * 
	 * @param name
	 * @param hostname
	 * @param user
	 * @param password
	 * @param verbose
	 * @throws Exception
	 */
	public static void deleteDatabase(String name, String hostname,
			String user, String password, boolean verbose) throws Exception {
		Connection databaseConnection = establishConnection(name, hostname,
				user, password);
		checkForOtherConnections(databaseConnection);
		deleteDatasetOids(databaseConnection);
		deleteDataDefOids(databaseConnection);
		databaseConnection.close();
		Connection templateConnection = establishConnection(templateName,
				hostname, user, password);
		dropDatabase(templateConnection, name);
		templateConnection.close();
		if (verbose)
			System.out.println("Database " + name + " dropped");
	}

	/**
	 * Executes a sql statement
	 * 
	 * @param dbCon
	 * @param statement
	 * @throws Exception
	 */
	public static void executeSQL(Connection dbCon, String statement)
			throws Exception {
		Statement stmt = dbCon.createStatement();
		try {
			stmt.execute(statement);
		} catch (Exception me) {
			dbCon.close();
			throw new MobbedException("Could not execute sql statement\n"
					+ me.getMessage());
		}
	}

	/**
	 * Creates a database
	 * 
	 * @param dbCon
	 * @throws Exception
	 */
	private static void createDatabase(Connection dbCon, String name)
			throws Exception {
		String sql = "CREATE DATABASE " + name;
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(sql);
			pStmt.execute();
		} catch (Exception me) {
			dbCon.close();
			throw new MobbedException("Could not create database " + name
					+ "\n" + me.getMessage());
		}
	}

	/**
	 * Creates database tables given a path to a sql file
	 * 
	 * @param dbCon
	 * @param path
	 * @throws Exception
	 */
	private static void createTables(Connection dbCon, String path)
			throws Exception {
		DataInputStream in = null;
		Statement stmt = dbCon.createStatement();
		try {
			File file = new File(path);
			byte[] buffer = new byte[(int) file.length()];
			in = new DataInputStream(new FileInputStream(file));
			in.readFully(buffer);
			in.close();
			String result = new String(buffer);
			String[] tables = result.split(";");
			for (int i = 0; i < tables.length; i++)
				stmt.execute(tables[i]);
		} catch (Exception me) {
			throw new MobbedException("Could not execute sql code\n"
					+ me.getMessage());
		}
	}

	/**
	 * Deletes oids in data_defs table
	 * 
	 * @param dbCon
	 * @throws Exception
	 */
	private static void deleteDataDefOids(Connection dbCon) throws Exception {
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			String qry = "SELECT DATA_DEF_OID FROM DATA_DEFS WHERE DATA_DEF_OID IS NOT NULL";
			Statement stmt = dbCon.createStatement();
			ResultSet rs = stmt.executeQuery(qry);
			while (rs.next()) {
				lobj.unlink(rs.getLong(1));
			}
		} catch (Exception me) {
			dbCon.close();
			throw new MobbedException(
					"Could not delete oids in data defs table\n"
							+ me.getMessage());
		}
	}

	/**
	 * Deletes oids in datasets table
	 * 
	 * @param dbCon
	 * @throws Exception
	 */
	private static void deleteDatasetOids(Connection dbCon) throws Exception {
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			String qry = "SELECT DATASET_OID FROM DATASETS WHERE DATASET_OID IS NOT NULL";
			Statement stmt = dbCon.createStatement();
			ResultSet rs = stmt.executeQuery(qry);
			while (rs.next())
				lobj.unlink(rs.getLong(1));
		} catch (Exception me) {
			dbCon.close();
			throw new MobbedException(
					"Could not delete oids in datasets table\n"
							+ me.getMessage());
		}
	}

	/**
	 * Drops a database given a name
	 * 
	 * @param dbCon
	 * @param name
	 * @throws Exception
	 */
	private static void dropDatabase(Connection dbCon, String name)
			throws Exception {
		String sql = "DROP DATABASE IF EXISTS " + name;
		try {
			Statement stmt = dbCon.createStatement();
			stmt.execute(sql);
		} catch (Exception me) {
			dbCon.close();
			throw new MobbedException("Could not drop database" + name + "\n"
					+ me.getMessage());
		}
	}

	/**
	 * Establishes a connection to a database
	 * 
	 * @param name
	 * @param hostname
	 * @param user
	 * @param password
	 * @return
	 * @throws Exception
	 */
	private static Connection establishConnection(String name, String hostname,
			String user, String password) throws Exception {
		Connection dbCon = null;
		String url = "jdbc:postgresql://" + hostname + "/" + name;
		try {
			Class.forName("org.postgresql.Driver");
			dbCon = DriverManager.getConnection(url, user, password);
		} catch (Exception me) {
			throw new MobbedException(
					"Could not establish a connection to database " + name
							+ "\n" + me.getMessage());
		}
		return dbCon;
	}

	/**
	 * Checks if a string is empty
	 * 
	 * @param s
	 * @return
	 */
	private static boolean isEmpty(String s) {
		boolean empty = true;
		if (s != null) {
			if (s.length() > 0)
				empty = false;
		}
		return empty;
	}

	/**
	 * Checks if a string array is empty
	 * 
	 * @param s
	 * @return
	 */
	private static boolean isEmpty(String[] s) {
		boolean empty = true;
		if (s != null) {
			if (s.length > 0)
				empty = false;
		}
		return empty;
	}
}