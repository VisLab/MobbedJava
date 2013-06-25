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

	private UUID attributeOrganizationalUuid;
	/**
	 * String path specifying structure for attribute on retrieval
	 */
	private String attributePath;
	/**
	 * The UUID of the attribute
	 */
	private UUID attributeUuid;
	/**
	 * The entity class of the attribute
	 */
	private String entityClass;
	/**
	 * The entity UUID of the attribute
	 */
	private UUID entityUuid;
	/**
	 * A prepared statement object that inserts attributes into the database
	 */
	private PreparedStatement insertStmt;
	/**
	 * The numeric value of the attribute
	 */
	private Double numericValue;
	/**
	 * The value of the attribute
	 */
	private String value;
	/**
	 * A query that inserts attributes into the database
	 */
	private static String insertQry = "INSERT INTO ATTRIBUTES "
			+ "(ATTRIBUTE_UUID, ATTRIBUTE_ENTITY_UUID, ATTRIBUTE_ENTITY_CLASS, ATTRIBUTE_ORGANIZATIONAL_UUID, ATTRIBUTE_PATH, "
			+ "ATTRIBUTE_NUMERIC_VALUE, ATTRIBUTE_VALUE)"
			+ " VALUES (?, ?, ?, ?, ?, ?, ?)";

	/**
	 * Creates a Attributes object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public Attributes(Connection dbCon) throws MobbedException {
		attributeUuid = null;
		entityUuid = null;
		entityClass = null;
		attributeOrganizationalUuid = null;
		attributePath = null;
		numericValue = null;
		value = null;
		try {
			insertStmt = dbCon.prepareStatement(insertQry);
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not create prepared statement for Attributes object\n"
							+ ex.getMessage());
		}
	}

	/**
	 * Adds a attribute to a batch.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addToBatch() throws MobbedException {
		try {
			insertStmt.setObject(1, attributeUuid, Types.OTHER);
			insertStmt.setObject(2, entityUuid, Types.OTHER);
			insertStmt.setString(3, entityClass);
			insertStmt.setObject(4, attributeOrganizationalUuid, Types.OTHER);
			insertStmt.setString(5, attributePath);
			insertStmt.setObject(6, numericValue);
			insertStmt.setString(7, value);
			insertStmt.addBatch();
		} catch (SQLException ex) {
			throw new MobbedException("Could not add attribute to batch\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Sets the class fields of a Attributes object.
	 * 
	 * @param attributeUuid
	 *            UUID of the attribute
	 * @param entityUuid
	 *            UUID of the entity associated with the attribute
	 * @param entityClass
	 *            name of the entity table associated with the attribute
	 * @param organizationalUuid
	 *            UUID of the organization associated with the attribute
	 * @param organizationalClass
	 *            name of the organizational table associated with the attribute
	 * @param attributePath
	 *            string path specifying structure for attribute on retrieval
	 * @param numericValue
	 *            numeric representation of the attribute value
	 * @param value
	 *            string representation of the attribute value
	 */
	public void reset(UUID attributeUuid, UUID entityUuid, String entityClass,
			UUID attributeOrganizationalUuid, String attributePath,
			Double numericValue, String value) {
		this.attributeUuid = attributeUuid;
		this.entityUuid = entityUuid;
		this.entityClass = entityClass;
		this.attributeOrganizationalUuid = attributeOrganizationalUuid;
		this.attributePath = attributePath;
		this.numericValue = numericValue;
		this.value = value;
	}

	/**
	 * Saves all attributes as a batch. All attributes in the batch will be
	 * successfully written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		try {
			insertStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save the attributes\n"
					+ ex.getMessage());
		}
	}
}
