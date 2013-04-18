package edu.utsa.mobbed;

import java.util.UUID;
import java.sql.*;

/**
 * Handler class for ELEMENTS table. Elements are considered to be data
 * collecting entities like channels in EEG. Class contains functions to store,
 * retrieve, delete or make other queries on the table. A record in the table is
 * uniquely identified by ELEMENT_UUID.
 * 
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */

/**
 * @author Arif Hossain, Kay Robbins
 * 
 */
public class Elements {
	private Attributes atb;
	private UUID datasetUuid;
	private Structures dataStruct;
	private Connection dbCon;
	private String[] elementDescriptions;
	private String elementField;
	private String[] elementLabels;
	private long[] elementPositions;
	private Structures elementStruct;
	private UUID[] elementUuids;
	private String groupLabel;
	private UUID groupUuid;
	private PreparedStatement insertStmt;
	private String modalityName;
	private static final String insertElementQry = "INSERT INTO ELEMENTS "
			+ "(ELEMENT_UUID, ELEMENT_LABEL, ELEMENT_ORGANIZATIONAL_UUID, ELEMENT_ORGANIZATIONAL_CLASS, ELEMENT_PARENT_UUID, ELEMENT_POSITION, ELEMENT_DESCRIPTION) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?)";

	/**
	 * Creates a new element object
	 * 
	 * @param dbCon
	 * @throws Exception
	 */
	public Elements(Connection dbCon) throws Exception {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.modalityName = null;
		this.groupLabel = null;
		this.groupUuid = null;
		this.elementField = null;
		this.elementLabels = null;
		this.elementDescriptions = null;
		this.elementPositions = null;
		this.dataStruct = null;
	}

	/**
	 * Adds the attribute
	 * 
	 * @param fieldName
	 * @param numericValue
	 * @param value
	 * @throws Exception
	 */
	public void addAttribute(String fieldName, Double[] numericValue,
			String[] value) throws Exception {
		String entityClass = "elements";
		String organizationalClass = "datasets";
		elementStruct = Structures.retrieve(dbCon, elementField,
				dataStruct.getStructureUuid(), true);
		addNewStructure(fieldName);
		UUID structureUUID = elementStruct.getChildrenByName(fieldName);
		for (int i = 0; i < elementLabels.length; i++) {
			atb.reset(UUID.randomUUID(), elementUuids[i], entityClass,
					datasetUuid, organizationalClass, structureUUID,
					numericValue[i], value[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Adds a element to the batch
	 * 
	 * @throws Exception
	 */
	public void addElements() throws Exception {
		insertStmt = dbCon.prepareStatement(insertElementQry);
		String organizationalClass = "datasets";
		if (groupLabel != null) {
			// set query parameters for element a group parent
			groupUuid = UUID.randomUUID();
			insertStmt.setObject(1, groupUuid, Types.OTHER);
			insertStmt.setObject(2, groupLabel);
			insertStmt.setObject(3, datasetUuid);
			insertStmt.setString(4, organizationalClass);
			insertStmt.setObject(5, groupUuid, Types.OTHER);
			insertStmt.setInt(6, -1);
			insertStmt.setObject(7, groupLabel);
			insertStmt.addBatch();
		}
		elementUuids = new UUID[elementLabels.length];
		for (int i = 0; i < elementLabels.length; i++) {
			elementUuids[i] = UUID.randomUUID();
			insertStmt.setObject(1, elementUuids[i], Types.OTHER);
			insertStmt.setObject(2, elementLabels[i]);
			insertStmt.setObject(3, datasetUuid);
			insertStmt.setString(4, organizationalClass);
			insertStmt.setObject(5, groupUuid, Types.OTHER);
			insertStmt.setLong(6, elementPositions[i]);
			insertStmt.setObject(7, elementDescriptions[i]);
			insertStmt.addBatch();
		}
	}

	/**
	 * Adds a new structure
	 * 
	 * @param fieldName
	 * @param handler
	 * @throws Exception
	 */
	private void addNewStructure(String fieldName) throws Exception {
		if (!elementStruct.containsChild(fieldName)) {
			Structures newStruct = new Structures(dbCon);
			newStruct.reset(UUID.randomUUID(), fieldName,
					elementStruct.getStructureUuid());
			newStruct.save();
			elementStruct = Structures.retrieve(dbCon, elementField,
					dataStruct.getStructureUuid(), true);
		}
	}

	/**
	 * Sets class fields
	 * 
	 * @param datasetUuid
	 * @param elementField
	 * @param defaultFields
	 * @param groupLabel
	 * @param elementLabels
	 * @param elementDescriptions
	 * @param elementPositions
	 * @throws Exception
	 */
	public void reset(String datasetUuid, String elementField,
			String[] defaultFields, String groupLabel, String[] elementLabels,
			String[] elementDescriptions, long[] elementPositions)
			throws Exception {
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.groupLabel = groupLabel;
		this.elementField = elementField;
		this.elementLabels = elementLabels;
		this.elementDescriptions = elementDescriptions;
		this.elementPositions = elementPositions;
		atb = new Attributes(dbCon);
		modalityName = Structures.retrieveModalityName(dbCon,
				UUID.fromString(datasetUuid));
		dataStruct = Structures.retrieve(dbCon, modalityName, null, false);
	} // reset

	/**
	 * Retrieves the element count
	 * 
	 * @param dbCon
	 * @param datadefUuid
	 * @return
	 * @throws Exception
	 */
	public static int getElementCount(Connection dbCon, String datadefUuid)
			throws Exception {
		int elementCount = 0;
		String countQry = "SELECT array_length(numeric_stream_data_value, 1) from numeric_streams where NUMERIC_STREAM_DEF_UUID = ? LIMIT 1";
		PreparedStatement pstmt = dbCon.prepareStatement(countQry);
		pstmt.setObject(1, datadefUuid, Types.OTHER);
		ResultSet rs = pstmt.executeQuery();
		rs = pstmt.executeQuery();
		if (rs.next())
			elementCount = rs.getInt("array_length");
		return elementCount;
	}

	/**
	 * Saves all elements as a batch
	 * 
	 * @throws Exception
	 */
	public void save() throws Exception {
		try {
			insertStmt.executeBatch();
			atb.save();
		} catch (Exception ex) {
			throw new MobbedException("Could not save elements");
		}
	}

}
