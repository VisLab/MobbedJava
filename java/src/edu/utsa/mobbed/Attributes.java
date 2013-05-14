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
	 * Creates a Attributes object
	 * 
	 * @param dbCon
	 *            - a connection to a Mobbed database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public Attributes(Connection dbCon) throws MobbedException {
		attributeUuid = null;
		entityUuid = null;
		entityClass = null;
		organizationalUuid = null;
		organizationalClass = null;
		structureUuid = null;
		attributeNumericValue = null;
		attributeValue = null;
		try {
			insertStmt = dbCon.prepareStatement(insertQry);
		} catch (SQLException ex) {
			throw new MobbedException("Could not create a Attributes object\n"
					+ ex.getNextException().getMessage());
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
			insertStmt.setObject(4, organizationalUuid, Types.OTHER);
			insertStmt.setString(5, organizationalClass);
			insertStmt.setObject(6, structureUuid, Types.OTHER);
			insertStmt.setObject(7, attributeNumericValue);
			insertStmt.setString(8, attributeValue);
			insertStmt.addBatch();
		} catch (SQLException ex) {
			throw new MobbedException("Could not add attribute to batch\n"
					+ ex.getNextException().getMessage());
		}
	}

	/**
	 * Sets the class fields of a Attributes object
	 * 
	 * @param attributeUuid
	 *            - UUID of the attribute
	 * @param entityUuid
	 *            - UUID of the entity associated with the attribute
	 * @param entityClass
	 *            - name of the entity table associated with the attribute
	 * @param organizationalUuid
	 *            - UUID of the organization associated with the attribute
	 * @param organizationalClass
	 *            - name of the organizational table associated with the
	 *            attribute
	 * @param structureUuid
	 *            - UUID of the structure associated with the attribute
	 * @param attributeNumericValue
	 *            - numeric representation of the attribute value
	 * @param attributeValue
	 *            - string representation of the attribute value
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
			throw new MobbedException("Could not save attributes\n"
					+ ex.getNextException().getMessage());
		}
	}
}
