package edu.utsa.mobbed;

import java.util.UUID;
import java.sql.*;

/**
 * Handler class for ATTRIBUTES table. Any entity (dataset, event, element)
 * represent their properties as a set of attributes. All properties are stored
 * against and ENTITY_UUID and STRUCTURE_UUD. Class contains functions to store,
 * retrieve, delete or making any other queries on the table. A record in the
 * table is uniquely identified by ATTRIBUTE_UUID. A setProperties() function is
 * provided to set all the field values. To store more than one attribute, it is
 * recommended to use one object of this class and use setProperties()to reset
 * field values.
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */
public class Attributes {

	private UUID attributeUuid;
	private int batchCount;
	private UUID entityUuid;
	private UUID organizationalUuid;
	private UUID structureUuid;
	private Double attributeNumericValue;
	private String attributeValue;
	PreparedStatement insertStmt;
	public static String insertQry = "INSERT INTO ATTRIBUTES "
			+ "(ATTRIBUTE_UUID, ATTRIBUTE_ENTITY_UUID, ATTRIBUTE_ORGANIZATIONAL_UUID, "
			+ "ATTRIBUTE_STRUCTURE_UUID,  ATTRIBUTE_NUMERIC_VALUE, ATTRIBUTE_VALUE)"
			+ " VALUES (?,?,?,?,?,?)";

	/**
	 * Create a new Attributes object
	 * 
	 * @param dbCon
	 *            connection to the database
	 */
	public Attributes(Connection dbCon) throws Exception {
		insertStmt = dbCon.prepareStatement(insertQry);
		batchCount = 0;
		attributeUuid = null;
		entityUuid = null;
		organizationalUuid = null;
		structureUuid = null;
		attributeNumericValue = null;
		attributeValue = null;
	}

	/**
	 * Add the attribute object to the batch insert queries
	 * 
	 * @throws MobbedException
	 * 
	 * @throws Exception
	 */
	public void addToBatch() throws MobbedException {
		try {
			insertStmt.setObject(1, attributeUuid, Types.OTHER);
			insertStmt.setObject(2, entityUuid, Types.OTHER);
			insertStmt.setObject(3, organizationalUuid, Types.OTHER);
			insertStmt.setObject(4, structureUuid, Types.OTHER);
			insertStmt.setObject(5, attributeNumericValue);
			insertStmt.setString(6, attributeValue);
			insertStmt.addBatch();
			batchCount++;
		} catch (Exception ex) {
			throw new MobbedException("Could not add attribute to batch");
		}
	}

	public int getBatchCount() {
		return batchCount;
	}

	/**
	 * Set the properties of a Attribute
	 * 
	 * @param attributeUuid
	 *            UUID for ATTRIBUTE table
	 * @param entityUuid
	 *            UUID of the entity
	 * @param organizationalUuid
	 *            UUID of the dataset (
	 *            {@link edu.utsa.testmobbed.helpers.Datasets})
	 * @param structureUuid
	 *            UUID of the structure ({@link edu.utsa.mobbed.Structures})
	 * @param position
	 *            Position of the attribute's owner
	 * @param attributeValue
	 *            Value of the attribute
	 */
	public void reset(UUID attributeUuid, UUID entityUuid,
			UUID organizationalUuid, UUID structureUuid,
			Double attributeNumericValue, String attributeValue) {
		this.attributeUuid = attributeUuid;
		this.entityUuid = entityUuid;
		this.organizationalUuid = organizationalUuid;
		this.structureUuid = structureUuid;
		this.attributeNumericValue = attributeNumericValue;
		this.attributeValue = attributeValue;
	}

	/**
	 * Save Attributes by executing the batch insert queries
	 * 
	 * @param dbCon
	 *            connection to the database
	 * @return boolean - true for successful store, false otherwise
	 * @throws Exception
	 */
	public void save() throws Exception {
		try {
			insertStmt.executeBatch();
		} catch (Exception ex) {
			throw new MobbedException("Could not save attributes");
		}
	}
}
