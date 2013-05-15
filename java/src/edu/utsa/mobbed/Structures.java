package edu.utsa.mobbed;

import java.util.HashMap;
import java.util.UUID;
import java.sql.*;

/**
 * Handler class for STRUCTURES table. The STRUCTURES table encapsulates the
 * organization of the dataset for insertion into or extraction from a
 * particular access layer. Structures form a forest for categorization and
 * reconstruction.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */

public class Structures {

	private Connection dbCon;
	private HashMap<String, UUID> children;
	private UUID parentUuid;
	private String structureName;
	private UUID structureUuid;
	private String structurePath;
	private static final String insertQry = "INSERT INTO STRUCTURES "
			+ " (STRUCTURE_UUID, STRUCTURE_NAME, STRUCTURE_PARENT_UUID, STRUCTURE_PATH)"
			+ " VALUES (?, ?, ?, ?)";
	private static final String selectQry = "SELECT STRUCTURE_NAME, STRUCTURE_PARENT_UUID FROM STRUCTURES "
			+ "WHERE STRUCTURE_UUID = ?";

	/**
	 * Creates a Structures object.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 */
	public Structures(Connection dbCon) {
		this.dbCon = dbCon;
		structureUuid = null;
		structureName = null;
		parentUuid = null;
		structurePath = null;
		children = new HashMap<String, UUID>();
	}

	/**
	 * Adds a child structure to the hashmap.
	 * 
	 * @param childName
	 *            - the name of the child structure
	 * @param childUuid
	 *            - the UUID of the child structure
	 */
	public void addChild(String childName, UUID childUuid) {
		children.put(childName, childUuid);
	}

	/**
	 * Checks if the hashmap contains the child structure.
	 * 
	 * @param childName
	 *            - the name of the child structure
	 * @return true if the child structure is in the hashmap, false if otherwise
	 */
	public boolean containsChild(String childName) {
		return children.containsKey(childName);
	}

	/**
	 * Gets the UUID of the child structure.
	 * 
	 * @param childName
	 *            - the name of the child structure
	 * @return the UUID of the child structure
	 */
	public UUID getChildStructUuid(String childName) {
		return children.get(childName);
	}

	/**
	 * Gets the UUID of the structure.
	 * 
	 * @return the UUID of the structure
	 */
	public UUID getStructureUuid() {
		return structureUuid;
	}

	/**
	 * Sets the class fields of a Structures object.
	 * 
	 * @param structureUuid
	 *            - the UUID of the structure
	 * @param structureName
	 *            - the name of the structure
	 * @param parentUuid
	 *            UUID of the parent structure
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(UUID structureUuid, String structureName, UUID parentUuid)
			throws MobbedException {
		this.structureUuid = structureUuid;
		this.structureName = structureName;
		this.parentUuid = parentUuid;
		structurePath = generateStructurePath();
	}

	/**
	 * Saves a structure to the database.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			insertStmt.setObject(1, structureUuid, Types.OTHER);
			insertStmt.setString(2, structureName);
			insertStmt.setObject(3, parentUuid, Types.OTHER);
			insertStmt.setString(4, structurePath);
			insertStmt.execute();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save structure\n"
					+ ex.getNextException().getMessage());
		}
	}

	/**
	 * Generates a structure path based on it's hierarchy.
	 * 
	 * @return the path of the structure
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String generateStructurePath() throws MobbedException {
		String structurePath = "/" + structureName;
		UUID currentParentUuid = parentUuid;
		try {
			PreparedStatement pstmt = dbCon.prepareStatement(selectQry);
			ResultSet rs = null;
			while (!UUID.fromString(ManageDB.noParentUuid).equals(
					currentParentUuid)) {
				pstmt.setObject(1, currentParentUuid);
				rs = pstmt.executeQuery();
				if (rs.next()) {
					structurePath = "/" + rs.getString(1) + structurePath;
					currentParentUuid = UUID.fromString(rs.getString(2));
				}
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not generate the structure path\n"
					+ ex.getNextException().getMessage());
		}
		return structurePath;
	}

	/**
	 * Retrieves the children of the structure from the database.
	 * 
	 * @param dbCon
	 *            - a connection to a Mobbed database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void retrieveChildren(Connection dbCon) throws MobbedException {
		String query = "SELECT STRUCTURE_NAME, STRUCTURE_UUID FROM STRUCTURES WHERE STRUCTURE_PARENT_UUID = ?";
		try {
			PreparedStatement pstmt = dbCon.prepareStatement(query);
			pstmt.setObject(1, structureUuid, Types.OTHER);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next())
				children.put(rs.getString(1), UUID.fromString(rs.getString(2)));
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not retrieve structure's children\n"
							+ ex.getNextException().getMessage());
		}
	}

	/**
	 * Retrieves an structure with its children.
	 * 
	 * @param dbCon
	 *            - a connection to a Mobbed database
	 * @param structureName
	 *            - the name of the structure
	 * @param parentUuid
	 *            - the UUID of the parent structure
	 * @param retrieveChildren
	 *            - retrieves the children of the structure if true
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static Structures retrieve(Connection dbCon, String structureName,
			UUID parentUuid, boolean retrieveChildren) throws MobbedException {
		String query = "SELECT STRUCTURE_UUID FROM STRUCTURES"
				+ " WHERE STRUCTURE_NAME = ? AND STRUCTURE_PARENT_UUID = ?";
		Structures s = new Structures(dbCon);
		try {
			PreparedStatement pstmt = dbCon.prepareStatement(query);
			pstmt.setString(1, structureName);
			pstmt.setObject(2, parentUuid, Types.OTHER);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next())
				s.reset(UUID.fromString(rs.getString(1)), structureName,
						parentUuid);
			else {
				s.reset(UUID.randomUUID(), structureName, parentUuid);
				s.save();
			}
			if (retrieveChildren)
				s.retrieveChildren(dbCon);
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve structure\n"
					+ ex.getNextException().getMessage());
		}
		return s;
	}
}
