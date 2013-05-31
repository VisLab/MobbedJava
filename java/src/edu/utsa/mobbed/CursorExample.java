package edu.utsa.mobbed;

import java.net.URLDecoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Solution 1 creates and opens a cursor and accesses the rows by calling fetch.
 * 
 * @author JCockfield
 * 
 */
public class CursorExample {

	/**
	 * @param args
	 * @throws MobbedException
	 */
	public static void main(String[] args) throws MobbedException {
		ManageDB md;
		String hostname = "localhost";
		String name = "testcursors";
		String password = "admin";
		String tablePath = null;
		String user = "postgres";
		boolean verbose = false;
		try {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/mobbed/mobbed.sql")
							.getPath(), "UTF-8");
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		}
		// scenarioCode(md);
		// multipleRetrieveRows(md);
		multipleRetrieveRowsWithCommits(md);

	}

	public static void scenarioCode(ManageDB md) throws MobbedException {
		System.out.println("***Scenario Code***");
		// Insert 100 datasets and make sure they are inserted
		insertDatasets(md, 100);
		printDatasetCount(md);
		// Set the fetch size
		md.setFetchSize(20);
		System.out.println("Fetch size: " + md.getFetchSize());
		// Call retrieveRows to fetch first set of datasets and print their
		// names
		String[][] datasetFetch1 = md.retrieveRows("datasets",
				Double.POSITIVE_INFINITY, "off", null, null, null, null,
				"datasetcursor");
		printDatasetNames(datasetFetch1);
		// Call next to fetch second set of datasets and print their names
		String[][] datasetFetch2 = md.next("datasetcursor");
		printDatasetNames(datasetFetch2);
		// Try to fetch from cursor that doesn't exist (throws exception)
		// String[][] datasetFetch3 = md.next("invalidcursor");
		// printDatasetNames(datasetFetch3);
		md.close();
	}

	public static void multipleRetrieveRows(ManageDB md) throws MobbedException {
		// Insert 100 datasets and make sure they are inserted
		insertDatasets(md, 100);
		printDatasetCount(md);
		// Insert 100 elements and make sure they are inserted
		insertElements(md, 100);
		printElementCount(md);
		// Set the fetch size
		md.setFetchSize(20);
		// Call retrieveRows to fetch first set of datasets and print their
		// names
		String[][] datasetFetch1 = md.retrieveRows("datasets",
				Double.POSITIVE_INFINITY, "off", null, null, null, null,
				"datasetcursor");
		printDatasetNames(datasetFetch1);
		// Call retrieveRows to fetch first set of elements and print their
		// names
		String[][] elementFetch1 = md.retrieveRows("elements",
				Double.POSITIVE_INFINITY, "off", null, null, null, null,
				"elementcursor");
		printElementLabels(elementFetch1);
		// Call next to fetch second set of datasets and print their names
		String[][] datasetFetch2 = md.next("datasetcursor");
		printDatasetNames(datasetFetch2);
		// Call next to fetch second set of elements and print their labels
		String[][] elementFetch2 = md.next("elementcursor");
		printElementLabels(elementFetch2);
	}

	public static void multipleRetrieveRowsWithCommits(ManageDB md)
			throws MobbedException {
		// Insert 100 datasets and make sure they are inserted
		insertDatasets(md, 100);
		printDatasetCount(md);
		// Insert 100 elements and make sure they are inserted
		insertElements(md, 100);
		printElementCount(md);
		// Set the fetch size
		md.setFetchSize(20);
		// Call retrieveRows to fetch first set of datasets and print their
		// names
		String[][] datasetFetch1 = md.retrieveRows("datasets",
				Double.POSITIVE_INFINITY, "off", null, null, null, null,
				"datasetcursor");
		printDatasetNames(datasetFetch1);
		// Do some stuff
		doSomeStuff(md);
		// Call retrieveRows to fetch first set of elements and print their
		// names
		String[][] elementFetch1 = md.retrieveRows("elements",
				Double.POSITIVE_INFINITY, "off", null, null, null, null,
				"elementcursor");
		printElementLabels(elementFetch1);
		// Call next to fetch second set of datasets and print their names
		String[][] datasetFetch2 = md.next("datasetcursor");
		printDatasetNames(datasetFetch2);
		// Do some more stuff
		doSomeMoreStuff(md);
		// Call next to fetch second set of elements and print their labels
		String[][] elementFetch2 = md.next("elementcursor");
		printElementLabels(elementFetch2);
	}

	public static void doSomeStuff(ManageDB md) throws MobbedException {
		// Store two contacts and commit them to the database (autocommit is
		// still false)
		String[][] contactValues = {
				{ null, "John", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" },
				{ null, "Jane", "Doe", "J", "123 Doe Drive", "456 Doe Drive",
						"Doeville", "Florida", "USA", "45353", "124-412-4574",
						"jdoe@email.com" } };
		md.addRows("contacts", md.getColumnNames("contacts"), contactValues,
				null, null);
		md.commit();
	}

	public static void doSomeMoreStuff(ManageDB md) throws MobbedException {
		// Store a dataset, element, and 3 attributes (autocommit is
		// set to true and then set back to false)
		md.setAutoCommit(true);
		String datasetValues[][] = { { null, null, null, "MANAGEDB_DATASET",
				null, null, null, "MANAGEDB_DATASET", null, null, null } };
		String[] datasetUuids = md.addRows("datasets",
				md.getColumnNames("datasets"), datasetValues, null, null);
		String[] elementLabels = { "channel 1" };
		String[] elementDescriptions = { "EEG channel: 1" };
		long[] elementPositions = { 1 };
		Elements element = new Elements(md.getConnection());
		element.reset("EEG", datasetUuids[0], "chanlocs", "EEG CAP",
				elementLabels, elementDescriptions, elementPositions);
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
		md.setAutoCommit(false);

	}

	// public static void createDatasetCursor(ManageDB md) throws
	// MobbedException {
	// md.createCursor("datasets", Double.POSITIVE_INFINITY, "off", null,
	// null, null, null, "datasetcursor");
	// }
	//
	// public static void createElementCursor(ManageDB md) throws
	// MobbedException {
	// md.createCursor("elements", Double.POSITIVE_INFINITY, "off", null,
	// null, null, null, "elementcursor");
	// }

	public static void insertDatasets(ManageDB md, int numDatasets)
			throws MobbedException {
		String sql = "INSERT INTO DATASETS (DATASET_NAME) VALUES (?)";
		try {
			PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
			for (int i = 0; i < numDatasets; i++) {
				pstmt.setString(1, "dataset " + (i + 1));
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			System.out.println("Insert: " + numDatasets + " datasets");
		} catch (SQLException ex) {
			throw new MobbedException("Could not insert datasets\n"
					+ ex.getMessage());
		}
		md.commit();
	}

	public static void insertElements(ManageDB md, int numElements)
			throws MobbedException {
		String sql = "INSERT INTO ELEMENTS (ELEMENT_LABEL, ELEMENT_POSITION) VALUES (?, ?)";
		try {
			PreparedStatement pstmt = md.getConnection().prepareStatement(sql);
			for (int i = 0; i < 100; i++) {
				pstmt.setString(1, "element " + (i + 1));
				pstmt.setLong(2, (i + 1));
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			System.out.println("Insert: " + numElements + " elements");
		} catch (SQLException ex) {
			throw new MobbedException("Could not insert elements\n"
					+ ex.getMessage());
		}
		md.commit();
	}

	public static void printDatasetCount(ManageDB md) throws MobbedException {
		String sql = "SELECT COUNT(*) FROM DATASETS";
		int count;
		try {
			Statement stmt = md.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			count = rs.getInt(1);
			System.out.println("Count: " + count + " datasets");
		} catch (SQLException ex) {
			throw new MobbedException("Could not get dataset count\n"
					+ ex.getMessage());
		}
	}

	public static void printDatasetNames(String[][] fetch) {
		System.out.println("Dataset Fetch");
		for (int i = 0; i < fetch.length; i++) {
			System.out.println("dataset name: " + fetch[i][3]);
		}
	}

	public static void printElementCount(ManageDB md) throws MobbedException {
		String sql = "SELECT COUNT(*) FROM DATASETS";
		int count;
		try {
			Statement stmt = md.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			count = rs.getInt(1);
			System.out.println("Count: " + count + " elements");
		} catch (SQLException ex) {
			throw new MobbedException("Could not get dataset count\n"
					+ ex.getMessage());
		}
	}

	public static void printElementLabels(String[][] fetch) {
		System.out.println("Element Fetch");
		for (int i = 0; i < fetch.length; i++) {
			System.out.println("element label: " + fetch[i][1]);
		}
	}

}
