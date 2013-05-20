package edu.utsa.mobbed;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.sql.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.xml.sax.InputSource;

/**
 * Handler class for DATADEFS table. The DATADEFS table identifies a piece of
 * data (an array, a blob, or a stream).
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */

public class Datadefs {

	/**
	 * Retrieves a blob from the database.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param filename
	 *            - the name of the file that the blob will be written to
	 * @param entityUuid
	 *            - the entity UUID that is associated
	 * @param additional
	 *            - true if the blob is addition data, false if otherwise
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void retrieveBlob(Connection dbCon, String filename,
			String entityUuid, boolean additional) throws MobbedException {
		String query;
		if (additional)
			query = "SELECT DATASET_OID FROM DATASETS WHERE DATASET_UUID = ? AND DATASET_OID IS NOT NULL LIMIT 1";
		else
			query = "SELECT DATADEF_OID FROM DATADEFS WHERE DATADEF_UUID = ? AND DATADEF_OID IS NOT NULL LIMIT 1";
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			PreparedStatement ps = dbCon.prepareStatement(query);
			ps.setObject(1, entityUuid, Types.OTHER);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				long oid = rs.getLong(1);
				LargeObject obj = lobj.open(oid, LargeObjectManager.READ);
				File file = new File(filename);
				FileOutputStream fos = new FileOutputStream(file);
				int fileLength = obj.size();
				byte buf[] = new byte[65536];
				int s, tl = 0;
				int readSize = Math.min(65536, fileLength - tl);
				while ((s = obj.read(buf, 0, readSize)) > 0) {
					fos.write(buf, 0, s);
					tl += s;
					readSize = Math.min(65536, fileLength - tl);
				}
				fos.close();
				obj.close();
			}
			rs.close();
			ps.close();
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve the blob\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Retrieves a numeric value data definition.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param datadefUuid
	 *            - the data definition UUID that is associated
	 * @return a array that contains the numeric value data
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static Object retrieveNumericValue(Connection dbCon,
			String datadefUuid) throws MobbedException {
		Object numericData = null;
		String selectQry = "SELECT NUMERIC_VALUE_DATA_VALUE FROM NUMERIC_VALUES WHERE NUMERIC_VALUE_DEF_UUID = ?";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(selectQry);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			ResultSet rs = insertStmt.executeQuery();
			if (rs.next())
				numericData = rs.getArray(1).getArray();
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve the numeric values\n"
					+ ex.getMessage());
		}
		return numericData;
	}

	/**
	 * Retrieves a xml value data definition.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param datadefUuid
	 *            - the data definition UUID that is associated
	 * @return a string that contains the xml value data
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static String retrieveXMLValue(Connection dbCon, String datadefUuid)
			throws MobbedException {
		String xmlData = null;
		String selectQry = "SELECT XML_VALUE_DATA_VALUE FROM XML_VALUES WHERE XML_VALUE_DEF_UUID = ?";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(selectQry);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			ResultSet rs = insertStmt.executeQuery();
			if (rs.next())
				xmlData = rs.getString(1);
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve the xml values\n"
					+ ex.getMessage());
		}
		return xmlData;
	}

	/**
	 * Stores a blob in the database.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param filename
	 *            - the name of the file that contains the blob
	 * @param entityUuid
	 *            - the entity UUID that is associated
	 * @param additional
	 *            - true if the blob is addition data, false if otherwise
	 * @return a oid associated with the blob stored
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static long storeBlob(Connection dbCon, String filename,
			String entityUuid, boolean additional) throws MobbedException {
		long oid = 0;
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			oid = lobj.createLO();
			LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
			File file = new File(filename);
			FileInputStream fis = new FileInputStream(file);
			long fileLength = file.length();
			byte[] buff = new byte[65536];
			int s, tl = 0;
			int readSize = (int) Math.min(65536, fileLength - tl);
			while ((s = fis.read(buff, 0, readSize)) > 0) {
				obj.write(buff, 0, s);
				tl += s;
				readSize = (int) Math.min(65536, fileLength - tl);
			}
			obj.close();
			fis.close();
			if (additional)
				createDatasetOid(dbCon, entityUuid, oid);
			else
				createdatadefOid(dbCon, entityUuid, oid);
		} catch (Exception ex) {
			throw new MobbedException("Could not save the blob\n"
					+ ex.getMessage());
		}
		return oid;
	}

	/**
	 * Stores a numeric value data definition.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param datadefUuid
	 *            - the data definition UUID that is associated
	 * @param numericValue
	 *            - the values for the numeric values data definition
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void storeNumericValue(Connection dbCon, String datadefUuid,
			Object[] numericValue) throws MobbedException {
		String insertQry = "INSERT INTO NUMERIC_VALUES (NUMERIC_VALUE_DEF_UUID, NUMERIC_VALUE_DATA_VALUE) VALUES (?,?)";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			Array sqlArray = dbCon.createArrayOf("float8", numericValue);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			insertStmt.setArray(2, sqlArray);
			insertStmt.execute();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save the numeric values\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Stores a xml value data definition.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param datadefUuid
	 *            - the data definition UUID that is associated
	 * @param xml
	 *            - the xml for the xml value data definition
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static void storeXMLValue(Connection dbCon, String datadefUuid,
			String xml) throws MobbedException {
		String insertQry = "INSERT INTO XML_VALUES (XML_VALUE_DEF_UUID, XML_VALUE_DATA_VALUE) VALUES (?,?)";
		validateXMLValue(xml);
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			insertStmt.setString(2, xml);
			insertStmt.executeUpdate();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save the xml data\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Creates a oid for a data definition.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param datadefUuid
	 *            - the data definition UUID that is associated
	 * @param oid
	 *            - the oid that was created from storing the blob
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void createdatadefOid(Connection dbCon, String datadefUuid,
			long oid) throws MobbedException {
		String updateQry = "UPDATE DATADEFS SET DATADEF_OID = ? WHERE DATADEF_UUID = ?";
		try {
			PreparedStatement stmt = dbCon.prepareStatement(updateQry);
			stmt.setLong(1, oid);
			stmt.setObject(2, datadefUuid, Types.OTHER);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			throw new MobbedException("Could not update the datadef oid\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Creates a oid for a dataset.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 * @param datasetUuid
	 *            - the UUID of the dataset that is associated
	 * @param oid
	 *            - the oid that was created from storing the blob
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void createDatasetOid(Connection dbCon, String datasetUuid,
			long oid) throws MobbedException {
		String updateQry = "UPDATE DATASETS SET DATASET_OID = ? WHERE DATASET_UUID = ?";
		try {
			PreparedStatement stmt = dbCon.prepareStatement(updateQry);
			stmt.setLong(1, oid);
			stmt.setObject(2, UUID.fromString(datasetUuid), Types.OTHER);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			throw new MobbedException("Could not update the dataset oid\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Validates a xml value data definition.
	 * 
	 * @param xml
	 *            - the xml value that will be validated
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static void validateXMLValue(String xml) throws MobbedException {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			InputSource is = new InputSource();
			Pattern p = Pattern.compile("^\\s*<\\?.*\\?>");
			Matcher m = p.matcher(xml);
			if (m.lookingAt())
				is.setCharacterStream(new StringReader(m.replaceFirst(m.group()
						+ " <fakeroot>")
						+ " </fakeroot>"));
			else
				is.setCharacterStream(new StringReader("<fakeroot> " + xml
						+ "</fakeroot>"));
			db.parse(is);
		} catch (Exception ex) {
			throw new MobbedException("XML is invalid\n" + ex.getMessage());
		}
	}

}
