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
	 * The labels of the elements
	 */
	private String[] elementLabels;
	/**
	 * The positions of the elements
	 */
	private long[] elementPositions;
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
	 * A query that inserts elements into the database
	 */
	private static final String insertElementQry = "INSERT INTO ELEMENTS "
			+ "(ELEMENT_UUID, ELEMENT_LABEL, ELEMENT_DATASET_UUID, ELEMENT_PARENT_UUID, ELEMENT_POSITION, ELEMENT_DESCRIPTION) "
			+ "VALUES (?, ?, ?, ?, ?, ?)";

	/**
	 * Creates a Elements object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public Elements(Connection dbCon) {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.groupLabel = null;
		this.groupUuid = null;
		this.elementLabels = null;
		this.elementDescriptions = null;
		this.elementPositions = null;
	}

	/**
	 * Add the attribute to a batch.
	 * 
	 * @param path
	 *            the path of the attribute
	 * @param numericValues
	 *            the numeric values of the attribute
	 * @param values
	 *            the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addAttribute(String path, Double[] numericValues,
			String[] values) throws MobbedException {
		for (int i = 0; i < elementLabels.length; i++) {
			atb.reset(UUID.randomUUID(), elementUuids[i], "elements",
					datasetUuid, path, numericValues[i], values[i]);
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
		elementUuids = new UUID[elementLabels.length];
		String[] stringElementUuids = new String[elementLabels.length];
		try {
			insertStmt = dbCon.prepareStatement(insertElementQry);
			if (groupLabel != null) {
				groupUuid = UUID.randomUUID();
				insertStmt.setObject(1, groupUuid, Types.OTHER);
				insertStmt.setObject(2, groupLabel);
				insertStmt.setObject(3, datasetUuid);
				insertStmt.setObject(4, null, Types.OTHER);
				insertStmt.setInt(5, -1);
				insertStmt.setObject(6, groupLabel);
				insertStmt.addBatch();
			}
			for (int i = 0; i < elementLabels.length; i++) {
				elementUuids[i] = UUID.randomUUID();
				stringElementUuids[i] = elementUuids[i].toString();
				insertStmt.setObject(1, elementUuids[i], Types.OTHER);
				insertStmt.setObject(2, elementLabels[i]);
				insertStmt.setObject(3, datasetUuid);
				insertStmt.setObject(4, groupUuid, Types.OTHER);
				insertStmt.setLong(5, elementPositions[i]);
				insertStmt.setObject(6, elementDescriptions[i]);
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
	public void reset(String datasetUuid, String groupLabel,
			String[] elementLabels, String[] elementDescriptions,
			long[] elementPositions) throws MobbedException {
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.groupLabel = groupLabel;
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

}
