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
	 * Creates a Strucutres object
	 * 
	 * @param dbCon
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
	 * Add a child to the hashmap
	 * 
	 * @param childName
	 * @param childUuid
	 */
	public void addChild(String childName, UUID childUuid) {
		children.put(childName, childUuid);
	}

	/**
	 * Checks if the hashmap contains the child
	 * 
	 * @param childName
	 * @return
	 */
	public boolean containsChild(String childName) {
		return children.containsKey(childName);
	}

	/**
	 * Gets the UUID of the child structure
	 * 
	 * @param childName
	 *            name of the child structure
	 * @return UUID UUID of the child structure
	 */
	public UUID getChildStructUuid(String childName) {
		return children.get(childName);
	}

	/**
	 * Gets the UUID of the structure
	 * 
	 * @return UUID UUID of the structure
	 */
	public UUID getStructureUuid() {
		return structureUuid;
	}

	/**
	 * Sets class fields
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
	 * Generates a structure path based on it's hierarchy
	 * 
	 * @return
	 * @throws MobbedException
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return structurePath;
	}

	/**
	 * Retrieves the children of the structure from the database
	 * 
	 * @throws Exception
	 */
	private void retrieveChildren(Connection dbCon) throws MobbedException {
		try {
			String query = "SELECT STRUCTURE_NAME, STRUCTURE_UUID FROM STRUCTURES WHERE STRUCTURE_PARENT_UUID = ?";
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
