package edu.utsa.mobbed;

import java.util.UUID;
import java.sql.*;

/**
 * Handler class for ELEMENTS table. Element entities identify time stamped data
 * streams. An element may correspond to a single entity or may contain sub
 * elements.
 * 
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */

public class Elements {
	private Attributes atb;
	private UUID datasetUuid;
	private Structures modalityStruct;
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
	 * Creates a Elements object
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
		this.modalityStruct = null;
	}

	/**
	 * Add the attribute to a batch
	 * 
	 * @param fieldName
	 * @param numericValue
	 * @param value
	 * @throws Exception
	 */
	public void addAttribute(String fieldName, Double[] numericValue,
			String[] value) throws Exception {
		addNewStructure(fieldName);
		UUID structureUUID = elementStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < elementLabels.length; i++) {
			atb.reset(UUID.randomUUID(), elementUuids[i], "elements",
					datasetUuid, "datasets", structureUUID, numericValue[i],
					value[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Add the elements to a batch
	 * 
	 * @throws Exception
	 */
	public String[] addElements() throws Exception {
		insertStmt = dbCon.prepareStatement(insertElementQry);
		String organizationalClass = "datasets";
		if (groupLabel != null) {
			groupUuid = UUID.randomUUID();
			insertStmt.setObject(1, groupUuid, Types.OTHER);
			insertStmt.setObject(2, groupLabel);
			insertStmt.setObject(3, datasetUuid);
			insertStmt.setString(4, organizationalClass);
			insertStmt.setObject(5, ManageDB.noParentUuid, Types.OTHER);
			insertStmt.setInt(6, -1);
			insertStmt.setObject(7, groupLabel);
			insertStmt.addBatch();
		}
		elementUuids = new UUID[elementLabels.length];
		String[] stringElementUuids = new String[elementLabels.length];
		for (int i = 0; i < elementLabels.length; i++) {
			elementUuids[i] = UUID.randomUUID();
			stringElementUuids[i] = elementUuids[i].toString();
			insertStmt.setObject(1, elementUuids[i], Types.OTHER);
			insertStmt.setObject(2, elementLabels[i]);
			insertStmt.setObject(3, datasetUuid);
			insertStmt.setString(4, organizationalClass);
			insertStmt.setObject(5, groupUuid, Types.OTHER);
			insertStmt.setLong(6, elementPositions[i]);
			insertStmt.setObject(7, elementDescriptions[i]);
			insertStmt.addBatch();
		}
		return stringElementUuids;
	}

	/**
	 * Sets class fields
	 * 
	 * @param datasetUuid
	 * @param elementField
	 * @param groupLabel
	 * @param elementLabels
	 * @param elementDescriptions
	 * @param elementPositions
	 * @throws Exception
	 */
	public void reset(String modalityName, String datasetUuid,
			String elementField, String groupLabel, String[] elementLabels,
			String[] elementDescriptions, long[] elementPositions)
			throws Exception {
		this.modalityName = modalityName;
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.groupLabel = groupLabel;
		this.elementField = elementField;
		this.elementLabels = elementLabels;
		this.elementDescriptions = elementDescriptions;
		this.elementPositions = elementPositions;
		atb = new Attributes(dbCon);
	} // reset

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

	/**
	 * Add a new structure if it doesn't already exist
	 * 
	 * @param fieldName
	 * @param handler
	 * @throws Exception
	 */
	private void addNewStructure(String fieldName) throws Exception {
		modalityStruct = Structures.retrieve(dbCon, modalityName,
				UUID.fromString(ManageDB.noParentUuid), false);
		elementStruct = Structures.retrieve(dbCon, elementField,
				modalityStruct.getStructureUuid(), true);
		if (!elementStruct.containsChild(fieldName)) {
			Structures newChild = new Structures(dbCon);
			newChild.reset(UUID.randomUUID(), fieldName,
					elementStruct.getStructureUuid());
			newChild.save();
			elementStruct.addChild(fieldName, newChild.getStructureUuid());
		}
	}

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

}
