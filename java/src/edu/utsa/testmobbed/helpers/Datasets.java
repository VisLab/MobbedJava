package edu.utsa.testmobbed.helpers;

import java.util.UUID;
import java.sql.*;

import edu.utsa.mobbed.MobbedException;


/**
 * Handler class for DATASETS table. A dataset contains all details of an
 * experiment. A typical dataset includes description of the recording elements,
 * activities and events and long stream of data samples. Class contains
 * functions to store, retrieve, delete or making any other queries on the
 * table. A record in the table is uniquely identified by DATASET_UUID. A
 * setProperties() function is provided to set all the field values. To store
 * more than one dataset, it is recommended to use one object of this class and
 * use setProperties()to reset field values.
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */
public class Datasets {

	private UUID datasetUuid;
	private UUID sessionUuid;
	private String datasetName;
	private UUID contactUuid;
	private String description;
	private String datasetNamespace;
	private int datasetVersion;
	private Connection dbCon;
	private boolean isUnique;
	private UUID modalityUuid;
	private UUID parentUuid;

	/**
	 * Create a new Datasets object
	 * 
	 * @param datasetName
	 *            name of the dataset
	 * @param contactUuid
	 *            UUID of the contact for this dataset as string
	 * @param description
	 *            Description of the dataset
	 * @param datasetNamespace
	 *            name space for this dataset
	 * @param datasetVersion
	 *            version of the dataset
	 * @param modalityMapUuid
	 *            UUID of ModalityMap for this dataset (see
	 *            {@link mobbed.Modalities})
	 * @param parentUuid
	 *            UUID of the parent dataset
	 */
	public Datasets(Connection dbCon) {
		this.dbCon = dbCon;
		datasetName = null;
		contactUuid = null;
		description = null;
		datasetNamespace = null;
		datasetVersion = 0;
		isUnique = true;
		modalityUuid = null;
		parentUuid = null;
		parentUuid = null;
		datasetUuid = null;
		sessionUuid = null;
	}

	public void reset(boolean isUnique, String datasetName, String contactUuid,
			String description, String datasetNamespace,
			String modalityMapUuid, String parentUuid) throws Exception {
		this.isUnique = isUnique;
		this.datasetName = datasetName;
		if (contactExists(dbCon, UUID.fromString(contactUuid)))
			this.contactUuid = UUID.fromString(contactUuid);
		this.description = description;
		this.datasetNamespace = datasetNamespace;
		this.modalityUuid = UUID.fromString(modalityMapUuid);
		if (parentUuid == null)
			this.parentUuid = null;
		else
			this.parentUuid = UUID.fromString(parentUuid);
		datasetUuid = UUID.randomUUID();
		sessionUuid = UUID.randomUUID();
		datasetVersion = retrieveLatesetVersion();
	}

	/**
	 * Save the Dataset object to the database
	 * 
	 * @return insertCount Number of inserted rows
	 * @throws Exception
	 * 
	 */
	public int save() throws Exception {
		int insertCount = 0;
		String insertQry = "INSERT INTO DATASETS "
				+ "(DATASET_UUID, DATASET_SESSION_UUID, DATASET_NAMESPACE, DATASET_NAME, DATASET_VERSION "
				+ ",DATASET_CONTACT_UUID, DATASET_DESCRIPTION, DATASET_MODALITY_UUID"
				+ ",DATASET_PARENT_UUID)" + " VALUES (?,?,?,?,?,?,?,?,?)";
		try {
			PreparedStatement insertStmt = dbCon.prepareStatement(insertQry);
			insertStmt.setObject(1, datasetUuid, Types.OTHER);
			insertStmt.setObject(2, sessionUuid, Types.OTHER);
			insertStmt.setString(3, datasetNamespace);
			insertStmt.setString(4, datasetName);
			insertStmt.setInt(5, datasetVersion);
			insertStmt.setObject(6, contactUuid, Types.OTHER);
			insertStmt.setString(7, description);
			insertStmt.setObject(8, modalityUuid, Types.OTHER);
			insertStmt.setObject(9, parentUuid, Types.OTHER);
			insertCount = insertStmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not save dataset");
		}
		return insertCount;
	}

	public int retrieveLatesetVersion() throws Exception {
		int version = 0;
		String selQry = "SELECT MAX(DATASET_VERSION) AS LATESTVERSION"
				+ " FROM DATASETS WHERE DATASET_NAMESPACE = ? AND DATASET_NAME = ?";
		PreparedStatement selStmt = dbCon.prepareStatement(selQry);
		selStmt.setString(1, datasetNamespace);
		selStmt.setString(2, datasetName);
		ResultSet rs = selStmt.executeQuery();
		if (rs.next()) {
			version = rs.getInt("latestversion");
			if (isUnique && version > 0)
				throw new MobbedException("dataset version is not unique");
			version = version + 1;
		}
		return version;
	}

	public static boolean contactExists(Connection dbCon, UUID contactUuid)
			throws Exception {
		boolean exists = false;
		String selectQry = "SELECT * FROM CONTACTS WHERE CONTACT_UUID = ?";
		try {
			PreparedStatement selectStmt = dbCon.prepareStatement(selectQry);
			selectStmt.setObject(1, contactUuid, Types.OTHER);
			ResultSet rs = selectStmt.executeQuery();
			exists = rs.next();
		} catch (SQLException ex) {
			throw new MobbedException("Contact uuid does not exist");
		}
		return exists;
	}

	/**
	 * get the UUID of the dataset
	 * 
	 * @return UUID - UUID of the dataset
	 */
	public UUID getDatasetUuid() {
		return datasetUuid;
	}

	/**
	 * get the ModalityMapUuid of the dataset
	 * 
	 * @return UUID - UUID of the ModalityMap for the dataset
	 */
	public UUID getModalityMapUuid() {
		return modalityUuid;
	}

}
