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
 * Handler class for DATA_DEFS table. Each row in DATA_DEF usually indicates a
 * stream of data either stored as file or as numeric values. Each datadef also
 * contains the mapping format for that data stream. If the data stream is
 * stored as a file object, the file OID is also stored in this table.
 * 
 * @author Arif Hossain
 * 
 */

public class Datadefs {

	/**
	 * Creates a oid for a data defs row
	 * 
	 * @param dbCon
	 * @param datadefUuid
	 * @param oid
	 * @return
	 * @throws Exception
	 */
	private static int createdatadefOid(Connection dbCon, String datadefUuid,
			long oid) throws Exception {
		int updateCount = 0;
		String updateQry = "UPDATE DATADEFS SET DATADEF_OID = ? WHERE DATADEF_UUID = ?";
		try {
			PreparedStatement stmt = dbCon.prepareStatement(updateQry);
			stmt.setLong(1, oid);
			stmt.setObject(2, datadefUuid, Types.OTHER);
			updateCount = stmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not update datadef oid");
		}
		return updateCount;
	}

	/**
	 * Creates a oid for a datasets row
	 * 
	 * @param dbCon
	 * @param datasetUuid
	 * @param oid
	 * @return
	 * @throws Exception
	 */
	private static int createDatasetOid(Connection dbCon, String datasetUuid,
			long oid) throws Exception {
		int updateCount = 0;
		String updateQry = "UPDATE DATASETS SET DATASET_OID = ? WHERE DATASET_UUID = ?";
		try {
			PreparedStatement stmt = dbCon.prepareStatement(updateQry);
			stmt.setLong(1, oid);
			stmt.setObject(2, UUID.fromString(datasetUuid), Types.OTHER);
			updateCount = stmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not update dataset oid");
		}
		return updateCount;
	}

	/**
	 * Retireves a blob
	 * 
	 * @param dbCon
	 * @param pathName
	 * @param entityUuid
	 * @param additional
	 * @throws Exception
	 */
	public static void retrieveBlob(Connection dbCon, String pathName,
			String entityUuid, boolean additional) throws Exception {
		String query = "";
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
				File file = new File(pathName);
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
			throw new MobbedException("Could not retrieve blob");
		}
	}

	/**
	 * Retrieves a numeric value array
	 * 
	 * @param dbCon
	 * @param datadefUuid
	 * @return
	 * @throws Exception
	 */
	public static Object retrieveNumericValue(Connection dbCon,
			String datadefUuid) throws Exception {
		Object numericData = null;
		String selectQry = "SELECT NUMERIC_VALUE FROM NUMERIC_VALUES WHERE NUMERIC_VALUE_DEF_UUID = ?";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(selectQry);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			ResultSet rs = insertStmt.executeQuery();
			if (rs.next())
				numericData = rs.getArray(1).getArray();
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve numeric values");
		}
		return numericData;
	}

	/**
	 * Retrieves xml
	 * 
	 * @param dbCon
	 * @param datadefUuid
	 * @return
	 * @throws Exception
	 */
	public static String retrieveXMLValue(Connection dbCon, String datadefUuid)
			throws Exception {
		String xmlData = null;
		String selectQry = "SELECT XML_VALUE FROM XML_VALUES WHERE XML_VALUE_DEF_UUID = ?";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(selectQry);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			ResultSet rs = insertStmt.executeQuery();
			if (rs.next())
				xmlData = rs.getString(1);
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve xml data");
		}
		return xmlData;
	}

	/**
	 * Stores a blob
	 * 
	 * @param dbCon
	 * @param pathName
	 * @param entityUuid
	 * @param additional
	 * @return
	 * @throws Exception
	 */
	public static long storeBlob(Connection dbCon, String pathName,
			String entityUuid, boolean additional) throws Exception {
		long oid = 0;
		try {
			LargeObjectManager lobj = ((org.postgresql.PGConnection) dbCon)
					.getLargeObjectAPI();
			oid = lobj.createLO();
			LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
			File file = new File(pathName);
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
			throw new MobbedException("Could not save blob");
		}
		return oid;
	}

	/**
	 * Stores numeric value array
	 * 
	 * @param dbCon
	 * @param datadefUuid
	 * @param numericValue
	 * @throws Exception
	 */
	public static void storeNumericValue(Connection dbCon, String datadefUuid,
			Object[] numericValue) throws Exception {
		String insertQry = "INSERT INTO NUMERIC_VALUES (NUMERIC_VALUE_DEF_UUID, NUMERIC_VALUE) VALUES (?,?)";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			Array sqlArray = dbCon.createArrayOf("float8", numericValue);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			insertStmt.setArray(2, sqlArray);
			insertStmt.execute();
		} catch (Exception ex) {
			throw new MobbedException("Could not save numeric values");
		}
	}

	/**
	 * Stores xml
	 * 
	 * @param dbCon
	 * @param datadefUuid
	 * @param xml
	 * @throws Exception
	 */
	public static void storeXMLValue(Connection dbCon, String datadefUuid,
			String xml) throws Exception {
		String insertQry = "INSERT INTO XML_VALUES (XML_VALUE_DEF_UUID, XML_VALUE) VALUES (?,?)";
		try {
			validateXMLValue(xml);
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			insertStmt.setObject(1, UUID.fromString(datadefUuid));
			insertStmt.setString(2, xml);
			insertStmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not save xml data");
		}
	}

	/**
	 * Validates xml
	 * 
	 * @param xml
	 * @throws Exception
	 */
	private static void validateXMLValue(String xml) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		InputSource is = new InputSource();
		Pattern p = Pattern.compile("^\\s*<\\?.*\\?>");
		Matcher m = p.matcher(xml);
		// has xml header
		if (m.lookingAt())
			is.setCharacterStream(new StringReader(m.replaceFirst(m.group()
					+ " <fakeroot>")
					+ " </fakeroot>"));
		// doesn't have xml header
		else
			is.setCharacterStream(new StringReader("<fakeroot> " + xml
					+ "</fakeroot>"));
		db.parse(is);
	}

}
