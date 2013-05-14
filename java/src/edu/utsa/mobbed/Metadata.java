package edu.utsa.mobbed;

import java.sql.Connection;
import java.util.UUID;

/**
 * Stores metadata information associated with a dataset.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class Metadata {

	private Attributes atb;
	private UUID datasetUuid;
	private Structures modalityStruct;
	private Connection dbCon;
	private String metadataField;
	private Structures metamodalityStruct;
	private String modalityName;

	/**
	 * Creates a Metadata object
	 * 
	 * @param dbCon
	 *            - a connection to a Mobbed database
	 */
	public Metadata(Connection dbCon) {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.metadataField = null;
		this.modalityName = null;
	}

	/**
	 * Adds a new attribute
	 * 
	 * @param fieldName
	 *            - name of the field
	 * @param numericValues
	 *            - the numeric values of the attribute
	 * @param values
	 *            - the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws MobbedException {
		addNewStructure(fieldName);
		UUID structureUUID = metamodalityStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < numericValues.length; i++) {
			atb.reset(UUID.randomUUID(), null, null, datasetUuid, "datasets",
					structureUUID, numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Sets the class fields of a Metadata object
	 * 
	 * @param modalityName
	 *            - the name of the modality
	 * @param datasetUuid
	 *            - the UUID of the dataset
	 * @param metadataField
	 *            - the name of the field that contains the metadata
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(String modalityName, String datasetUuid,
			String metadataField) throws MobbedException {
		this.modalityName = modalityName;
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.metadataField = metadataField;
		atb = new Attributes(dbCon);
	}

	/**
	 * Saves all metadata as a batch. All metadata in the batch will be
	 * successfully written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		atb.save();
	}

	/**
	 * Adds a field to the structures table if it does not already exist.
	 * 
	 * @param fieldName
	 *            - the name of the field
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void addNewStructure(String fieldName) throws MobbedException {
		modalityStruct = Structures.retrieve(dbCon, modalityName,
				UUID.fromString(ManageDB.noParentUuid), true);
		metamodalityStruct = Structures.retrieve(dbCon, metadataField,
				modalityStruct.getStructureUuid(), true);
		if (!metamodalityStruct.containsChild(fieldName)) {
			Structures newChild = new Structures(dbCon);
			newChild.reset(UUID.randomUUID(), fieldName,
					metamodalityStruct.getStructureUuid());
			newChild.save();
			metamodalityStruct.addChild(fieldName, newChild.getStructureUuid());
		}
	}
}
