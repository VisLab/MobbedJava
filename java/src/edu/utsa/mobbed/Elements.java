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
	 *            - a connection to a Mobbed database
	 */
	public Elements(Connection dbCon) {
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
	 * 
	 * @param fieldName
	 *            - the field name of the attribute
	 * @param numericValues
	 *            - the numeric values of the attribute
	 * @param values
	 *            - the string values of the attribute
	 * @throws MobbedException
	 */
	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws MobbedException {
		addNewStructure(fieldName);
		UUID structureUUID = elementStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < elementLabels.length; i++) {
			atb.reset(UUID.randomUUID(), elementUuids[i], "elements",
					datasetUuid, "datasets", structureUUID, numericValues[i],
					values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Adds the elements to a batch. A group element will be added to the batch
	 * prior to the child elements if a group label is given.
	 * 
	 * @return the UUIDs of the elements that were added to the batch
	 * @throws MobbedException
	 */
	public String[] addElements() throws MobbedException {
		String organizationalClass = "datasets";
		elementUuids = new UUID[elementLabels.length];
		String[] stringElementUuids = new String[elementLabels.length];
		try {
			insertStmt = dbCon.prepareStatement(insertElementQry);
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
		} catch (SQLException ex) {
			throw new MobbedException("Could not add elements\n"
					+ ex.getNextException().getMessage());
		}
		return stringElementUuids;
	}

	/**
	 * Sets the class fields of a Elements object
	 * 
	 * @param modalityName
	 *            - the name of the modality associated with the elements
	 * @param datasetUuid
	 *            - the UUID of the dataset associated with the elements
	 * @param elementField
	 *            - the field that contains the elements
	 * @param groupLabel
	 *            - the label of the parent element
	 * @param elementLabels
	 *            - the labels of the elements
	 * @param elementDescriptions
	 *            - the descriptions of the elements
	 * @param elementPositions
	 *            - the positions of the elements. starting at 1, 2, ...
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
	 * Saves all elements as a batch. All elements in the batch will be
	 * successfully written or the operation will be aborted.
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
	 * Adds a field to the structures table if it does not already exist.
	 * 
	 * @param fieldName
	 *            - the name of the field
	 * @throws MobbedException
	 */
	private void addNewStructure(String fieldName) throws MobbedException {
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
	 * Retrieves the length of each array in the numeric stream. The length is
	 * equal to the number of elements in the stream.
	 * 
	 * @param dbCon
	 *            - a connection to a Mobbed database
	 * @param datadefUuid
	 *            - the UUID of the numeric stream data definition
	 * @return the length of each row in the numeric stream
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
