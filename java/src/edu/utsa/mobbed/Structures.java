package edu.utsa.mobbed;

import java.util.UUID;
import java.sql.*;
import java.util.Hashtable;

/**
 * Handler class for STRUCTURES table. A structure represents a property of any
 * entity. In the MoBBED structure, name of any property along with its parent
 * and child is stored in the STRUCTURES table. The actual property values are
 * stored in the ATTRIBUTES table. A structure is uniquely identified by a
 * STRUCUTURE_UUID.
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */

public class Structures {

	private Connection dbCon;
	private Hashtable<String, UUID> children;
	private UUID parentUuid;
	private String structureName;
	private UUID structureUuid;
	private String structurePath;
	private static final String insertQry = "INSERT INTO STRUCTURES "
			+ " (STRUCTURE_UUID, STRUCTURE_NAME, STRUCTURE_PARENT_UUID, STRUCTURE_PATH)"
			+ " VALUES (?, ?, ?, ?)";
	private static final String selectQry = "SELECT STRUCTURE_NAME, STRUCTURE_PARENT_UUID FROM STRUCTURES "
			+ "WHERE STRUCTURE_UUID = ?";

	public Structures(Connection dbCon) {
		this.dbCon = dbCon;
		structureUuid = null;
		structureName = null;
		parentUuid = null;
		structurePath = null;
		children = new Hashtable<String, UUID>();
	}

	/**
	 * Adds a child to the hash table
	 * 
	 * @param childName
	 * @param childUuid
	 */
	public void addChild(String childName, UUID childUuid) {
		children.put(childName, childUuid);
	}

	/**
	 * Check if the given child name is a children of the current structure
	 * 
	 * @param childName
	 *            Name of the child structure
	 * @return boolean true if given name is a child of this structure, false
	 *         otherwise.
	 */
	public boolean containsChild(String childName) {
		return children.containsKey(childName);
	}

	/**
	 * Returns UUID of the child structure
	 * 
	 * @param childName
	 *            name of the child structure
	 * @return UUID UUID of the child structure
	 */
	public UUID getChildStructUuid(String childName) {
		return children.get(childName);
	}

	/**
	 * Return UUID of the structure
	 * 
	 * @return UUID UUID of the structure
	 */
	public UUID getStructureUuid() {
		return structureUuid;
	}

	/**
	 * Create a structure object
	 * 
	 * @param structureUuid
	 *            UUID for STRUCTURE table
	 * @param structureName
	 *            Name of the structure
	 * @param structureHandler
	 *            Handler for this structure (see
	 *            {@link edu.utsa.mobbed.MobbedConstants} )
	 * @param parentUuid
	 *            UUID of the parent structure
	 */
	public void reset(UUID structureUuid, String structureName, UUID parentUuid)
			throws MobbedException {
		this.structureUuid = structureUuid;
		this.structureName = structureName;
		this.parentUuid = parentUuid;
		structurePath = generateStructurePath();
	}

	/**
	 * Saves a structure to the database
	 * 
	 * @throws Exception
	 */
	public void save() throws Exception {
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			insertStmt.setObject(1, structureUuid, Types.OTHER);
			insertStmt.setString(2, structureName);
			insertStmt.setObject(3, parentUuid, Types.OTHER);
			insertStmt.setString(4, structurePath);
			insertStmt.execute();
		} catch (Exception ex) {
			throw new MobbedException("Could not save structure");
		}
	}

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
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return structurePath;
	}

	/**
	 * Retrieve children of the current structure from database
	 * 
	 * @throws Exception
	 */
	private void retrieveChildren(Connection dbCon) throws Exception {
		try {
			String query = "SELECT STRUCTURE_NAME, STRUCTURE_UUID FROM STRUCTURES WHERE STRUCTURE_PARENT_UUID = ?";
			PreparedStatement pstmt = dbCon.prepareStatement(query);
			pstmt.setObject(1, structureUuid, Types.OTHER);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next())
				children.put(rs.getString(1), UUID.fromString(rs.getString(2)));
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve structure's children");
		}
	}

	/**
	 * Retrieves an structure with its children
	 * 
	 * @param dbCon
	 *            connection to the database
	 * @param structureName
	 *            Name of the structure
	 * @param parentUuid
	 *            UUID of the parent
	 * @param retrieveChildren
	 *            True if retrieval of children is required. False otherwise.
	 * @throws Exception
	 */
	public static Structures retrieve(Connection dbCon, String structureName,
			UUID parentUuid, boolean retrieveChildren) throws Exception {
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
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve structure");
		}
		return s;
	}

}
