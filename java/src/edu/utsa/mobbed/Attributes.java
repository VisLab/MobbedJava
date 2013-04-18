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
	private UUID entityUuid;
	private String entityClass;
	private UUID organizationalUuid;
	private String organizationalClass;
	private UUID structureUuid;
	private Double attributeNumericValue;
	private String attributeValue;
	private PreparedStatement insertStmt;
	public static String insertQry = "INSERT INTO ATTRIBUTES "
			+ "(ATTRIBUTE_UUID, ATTRIBUTE_ENTITY_UUID, ATTRIBUTE_ENTITY_CLASS, ATTRIBUTE_ORGANIZATIONAL_UUID, "
			+ "ATTRIBUTE_ORGANIZATIONAL_CLASS, ATTRIBUTE_STRUCTURE_UUID,  ATTRIBUTE_NUMERIC_VALUE, ATTRIBUTE_VALUE)"
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

	/**
	 * Create a new Attributes object
	 * 
	 * @param dbCon
	 *            connection to the database
	 */
	public Attributes(Connection dbCon) throws Exception {
		insertStmt = dbCon.prepareStatement(insertQry);
		attributeUuid = null;
		entityUuid = null;
		entityClass = null;
		organizationalUuid = null;
		organizationalClass = null;
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
			insertStmt.setString(3, entityClass);
			insertStmt.setObject(4, organizationalUuid, Types.OTHER);
			insertStmt.setString(5, organizationalClass);
			insertStmt.setObject(6, structureUuid, Types.OTHER);
			insertStmt.setObject(7, attributeNumericValue);
			insertStmt.setString(8, attributeValue);
			insertStmt.addBatch();
		} catch (Exception ex) {
			throw new MobbedException("Could not add attribute to batch");
		}
	}

	/**
	 * Sets the properties of an Attribute
	 * 
	 * @param attributeUuid
	 * @param entityUuid
	 * @param entityClass
	 * @param organizationalUuid
	 * @param organizationalClass
	 * @param structureUuid
	 * @param attributeNumericValue
	 * @param attributeValue
	 */
	public void reset(UUID attributeUuid, UUID entityUuid, String entityClass,
			UUID organizationalUuid, String organizationalClass,
			UUID structureUuid, Double attributeNumericValue,
			String attributeValue) {
		this.attributeUuid = attributeUuid;
		this.entityUuid = entityUuid;
		this.entityClass = entityClass;
		this.organizationalUuid = organizationalUuid;
		this.organizationalClass = organizationalClass;
		this.structureUuid = structureUuid;
		this.attributeNumericValue = attributeNumericValue;
		this.attributeValue = attributeValue;
	}

	/**
	 * Saves a batch of Attributes
	 * 
	 * @throws Exception
	 */
	public void save() throws MobbedException {
		try {
			insertStmt.executeBatch();
		} catch (Exception ex) {
			throw new MobbedException("Could not save attributes");
		}
	}
}
