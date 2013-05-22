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
	/**
	 * A Attributes object used to store attributes
	 */
	private Attributes atb;
	/**
	 * The dataset UUID of the elements
	 */
	private UUID datasetUuid;
	/**
	 * A connection to the database
	 */
	private Connection dbCon;
	/**
	 * The descriptions of the elements
	 */
	private String[] elementDescriptions;
	/**
	 * The field name of the elements
	 */
	private String elementField;
	/**
	 * The labels of the elements
	 */
	private String[] elementLabels;
	/**
	 * The positions of the elements
	 */
	private long[] elementPositions;
	/**
	 * The elements structure of the elements
	 */
	private Structures elementStruct;
	/**
	 * The UUIDs of the elements
	 */
	private UUID[] elementUuids;
	/**
	 * The group label of the elements
	 */
	private String groupLabel;
	/**
	 * The group UUID of the elements
	 */
	private UUID groupUuid;
	/**
	 * A prepared statement object that inserts elements into the database
	 */
	private PreparedStatement insertStmt;
	/**
	 * The modality name of the elements
	 */
	private String modalityName;
	/**
	 * The modality structure of the elements
	 */
	private Structures modalityStruct;
	/**
	 * A query that inserts elements into the database
	 */
	private static final String insertElementQry = "INSERT INTO ELEMENTS "
			+ "(ELEMENT_UUID, ELEMENT_LABEL, ELEMENT_ORGANIZATIONAL_UUID, ELEMENT_ORGANIZATIONAL_CLASS, ELEMENT_PARENT_UUID, ELEMENT_POSITION, ELEMENT_DESCRIPTION) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?)";

	/**
	 * Creates a Elements object.
	 * 
	 * @param dbCon
	 *            a connection to the database
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
	 * Add the attribute to a batch.
	 * 
	 * @param fieldName
	 *            the field name of the attribute
	 * @param numericValues
	 *            the numeric values of the attribute
	 * @param values
	 *            the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
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
				insertStmt.setObject(5, ManageDB.nullParentUuid, Types.OTHER);
				insertStmt.setInt(6, 1);
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
			throw new MobbedException("Could not add elements to the batch\n"
					+ ex.getMessage());
		}
		return stringElementUuids;
	}

	/**
	 * Sets the class fields of a Elements object.
	 * 
	 * @param modalityName
	 *            the name of the modality associated with the elements
	 * @param datasetUuid
	 *            the UUID of the dataset associated with the elements
	 * @param elementField
	 *            the field that contains the elements
	 * @param groupLabel
	 *            the label of the parent element
	 * @param elementLabels
	 *            the labels of the elements
	 * @param elementDescriptions
	 *            the descriptions of the elements
	 * @param elementPositions
	 *            the positions of the elements. starting at 1, 2, ...
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(String modalityName, String datasetUuid,
			String elementField, String groupLabel, String[] elementLabels,
			String[] elementDescriptions, long[] elementPositions)
			throws MobbedException {
		this.modalityName = modalityName;
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.groupLabel = groupLabel;
		this.elementField = elementField;
		this.elementLabels = elementLabels;
		this.elementDescriptions = elementDescriptions;
		this.elementPositions = elementPositions;
		atb = new Attributes(dbCon);
	}

	/**
	 * Saves all elements as a batch. All elements in the batch will be
	 * successfully written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		atb.save();
		try {
			insertStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save elements\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Adds a field to the structures table if it does not already exist.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void addNewStructure(String fieldName) throws MobbedException {
		modalityStruct = Structures.retrieve(dbCon, modalityName,
				UUID.fromString(ManageDB.nullParentUuid), false);
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

}
