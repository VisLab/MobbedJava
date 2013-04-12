package edu.utsa.mobbed;

import java.util.Enumeration;
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
	private static final String insertQry = "INSERT INTO STRUCTURES "
			+ " (STRUCTURE_UUID, STRUCTURE_NAME,  STRUCTURE_PARENT_UUID)"
			+ " VALUES (?,?,?)";

	public Structures(Connection dbCon) {
		this.dbCon = dbCon;
		this.structureUuid = null;
		this.structureName = null;
		this.parentUuid = null;
		children = new Hashtable<String, UUID>();
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
	public void reset(UUID structureUuid, String structureName, UUID parentUuid) {
		this.structureUuid = structureUuid;
		this.structureName = structureName;
		this.parentUuid = parentUuid;
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
	 * Return names of children structures
	 * 
	 * @return String[] 1D-array of children structure names
	 */
	public String[] getChildNames() {
		String[] childNames = new String[getChildrenCount()];
		Enumeration<String> e = children.keys();
		int index = 0;
		while (e.hasMoreElements())
			childNames[index++] = e.nextElement();
		return childNames;
	}

	/**
	 * Returns UUID of the child structure
	 * 
	 * @param childName
	 *            name of the child structure
	 * @return UUID UUID of the child structure
	 */
	public UUID getChildrenByName(String childName) {
		return children.get(childName);
	}

	/**
	 * Return number of children structure
	 * 
	 * @return int Number of children
	 */
	public int getChildrenCount() {
		return children.size();
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
		Structures s = null;
		String selectByNameAndParent = "SELECT * FROM STRUCTURES"
				+ " WHERE STRUCTURE_NAME = ? AND STRUCTURE_PARENT_UUID = ?";
		String selectByNameAndParentNull = "SELECT * FROM STRUCTURES"
				+ " WHERE STRUCTURE_NAME = ? AND STRUCTURE_PARENT_UUID IS NULL";
		try {
			ResultSet rs = null;
			if (parentUuid != null) {
				PreparedStatement select = dbCon
						.prepareStatement(selectByNameAndParent);
				select.setString(1, structureName);
				select.setObject(2, parentUuid, Types.OTHER);
				rs = select.executeQuery();
			} else {
				PreparedStatement select = dbCon
						.prepareStatement(selectByNameAndParentNull);
				select.setString(1, structureName);
				rs = select.executeQuery();
			}
			if (rs.next()) { // structure entry found in database
				s = new Structures(dbCon);
				s.reset(UUID.fromString(rs.getString("STRUCTURE_UUID")),
						structureName, parentUuid);
				if (retrieveChildren)
					s.retrieveChildren(dbCon);
			} else { // not found in db, need to store
				s = new Structures(dbCon);
				s.reset(UUID.randomUUID(), structureName, parentUuid);
				s.save();
			}
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve structure");
		}
		return s;
	}

	/**
	 * Retrieve children of the current structure from database
	 * 
	 * @throws Exception
	 */
	private void retrieveChildren(Connection dbCon) throws Exception {
		try {
			PreparedStatement childrenQry = dbCon
					.prepareStatement("SELECT STRUCTURE_NAME, STRUCTURE_UUID FROM STRUCTURES WHERE STRUCTURE_PARENT_UUID = ?");
			childrenQry.setObject(1, structureUuid, Types.OTHER);
			ResultSet rs = childrenQry.executeQuery();
			while (rs.next())
				children.put(rs.getString("STRUCTURE_NAME"),
						UUID.fromString(rs.getString("STRUCTURE_UUID")));
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve structure's children");
		}
	}

	public static String retrieveModalityName(Connection dbCon, UUID entityUuid)
			throws Exception {
		String parentName = "";
		String selectQry = "SELECT MODALITY_NAME FROM MODALITIES WHERE MODALITY_UUID IN "
				+ "(SELECT DATASET_MODALITY_UUID FROM DATASETS WHERE DATASET_UUID = ?)";
		try {
			PreparedStatement pstmt = dbCon.prepareStatement(selectQry);
			pstmt.setObject(1, entityUuid, Types.OTHER);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			parentName = rs.getString(1);
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve parent structure");
		}
		return parentName;
	}

	/**
	 * Save the structure in the database
	 * 
	 * @return int - number of rows inserted
	 * @throws Exception
	 */
	public int save() throws Exception {
		int insertCount = 0;

		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			insertStmt.setObject(1, structureUuid, Types.OTHER);
			insertStmt.setString(2, structureName);
			insertStmt.setObject(3, parentUuid, Types.OTHER);
			insertCount = insertStmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not save structure");
		}
		return insertCount;
	}

}
