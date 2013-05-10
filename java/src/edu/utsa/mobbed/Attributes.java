package edu.utsa.mobbed;

import java.util.UUID;
import java.sql.*;

/**
 * Handler class for ATTRIBUTES table. Attributes, which do not have timestamps,
 * may encapsulate entity metadata or other characteristics as well as time
 * independent data.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
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
	 * Create a Attributes object
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
	 * Add the attribute to a batch
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
	 * Sets class fields
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
	 * Saves all attributes as a batch
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
