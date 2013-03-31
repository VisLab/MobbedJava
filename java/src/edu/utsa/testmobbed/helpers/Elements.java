package edu.utsa.testmobbed.helpers;

import java.util.UUID;
import java.sql.*;

import edu.utsa.mobbed.MobbedConstants;
import edu.utsa.mobbed.MobbedException;




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
			+ "(ELEMENT_UUID, ELEMENT_LABEL, ELEMENT_PARENT_UUID, ELEMENT_POSITION, ELEMENT_DESCRIPTION) "
			+ "VALUES (?,?,?,?,?)";

	/**
	 * create a new Elements object
	 * 
	 * @param dbCon
	 *            connection to the database
	 * @param datasetUuid
	 *            UUID of the dataset
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
	 * 
	 * @param fieldNames
	 * @param fieldValues
	 * @throws Exception
	 */
	public void addAttribute(String fieldName, Double[] numericValue,
			String[] value) throws Exception {
		elementStruct = Structures.retrieve(dbCon, elementField,
				dataStruct.getStructureUuid(), true);
		addNewStructure(fieldName, MobbedConstants.HANDLER_REQUIRED);
		UUID structureUUID = elementStruct.getChildrenByName(fieldName);
		for (int i = 0; i < elementLabels.length; i++) {
			atb.reset(UUID.randomUUID(), elementUuids[i], datasetUuid,
					structureUUID, i, numericValue[i], value[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Add the element statements to the batch
	 * 
	 * @throws Exception
	 */
	public void addElements() throws Exception {
		insertStmt = dbCon.prepareStatement(insertElementQry);
		UUID parentUuid = datasetUuid;
		if (groupLabel != null) {
			// set query parameters for element a group parent
			groupUuid = UUID.randomUUID();
			insertStmt.setObject(1, groupUuid, Types.OTHER);
			insertStmt.setObject(2, groupLabel);
			insertStmt.setObject(3, datasetUuid, Types.OTHER);
			insertStmt.setInt(4, (-1));
			insertStmt.setObject(5, groupLabel);
			insertStmt.addBatch();
			parentUuid = groupUuid;
		}
		elementUuids = new UUID[elementLabels.length];
		for (int i = 0; i < elementLabels.length; i++) {
			elementUuids[i] = UUID.randomUUID(); // element uuid that attributes
			// use
			// set query parameters for element
			insertStmt.setObject(1, elementUuids[i], Types.OTHER);
			insertStmt.setObject(2, elementLabels[i]);
			insertStmt.setObject(3, parentUuid, Types.OTHER);
			insertStmt.setLong(4, elementPositions[i]);
			insertStmt.setObject(5, elementDescriptions[i]);
			insertStmt.addBatch();
		}
	}

	/**
	 * Add a new structure to the elementStruct
	 * 
	 * @param fieldName
	 *            name of the structure element to be added
	 * @param handler
	 *            - code for the integer
	 * @throws Exception
	 */
	private void addNewStructure(String fieldName, int handler)
			throws Exception {
		if (!elementStruct.containsChild(fieldName)) {
			Structures newStruct = new Structures(dbCon);
			newStruct.reset(UUID.randomUUID(), fieldName, handler,
					elementStruct.getStructureUuid());
			newStruct.save();
			elementStruct = Structures.retrieve(dbCon, elementField,
					dataStruct.getStructureUuid(), true);
		}
	}

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

	public static int getElementCount(Connection dbCon, String datadefUuid)
			throws Exception {
		int elementCount = 0;
		String countQry = "SELECT array_length(numeric_stream_data_value, 1) from numeric_streams where data_def_uuid = ? LIMIT 1";
		PreparedStatement pstmt = dbCon.prepareStatement(countQry);
		pstmt.setObject(1, datadefUuid, Types.OTHER);
		ResultSet rs = pstmt.executeQuery();
		rs = pstmt.executeQuery();
		if (rs.next())
			elementCount = rs.getInt("array_length");
		return elementCount;
	}

	/**
	 * save all the elements as a batch operation
	 * 
	 * @return boolean - true if successfully stored, false otherwise
	 */
	public void save() throws Exception {
		try {
			insertStmt.executeBatch();
			if (atb.getBatchCount() > 0)
				atb.save();
		} catch (Exception ex) {
			throw new MobbedException("Could not save elements");
		}
	}

}
