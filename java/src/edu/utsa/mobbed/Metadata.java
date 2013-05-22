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

	/**
	 * A Attributes object used to store attributes
	 */
	private Attributes atb;
	/**
	 * The dataset UUID of the metadata
	 */
	private UUID datasetUuid;
	/**
	 * A connection to the database
	 */
	private Connection dbCon;
	/**
	 * The field name of the metadata
	 */
	private String metadataField;
	/**
	 * The metadata struct of the metadata
	 */
	private Structures metadataStruct;
	/**
	 * The modality name of the metadata
	 */
	private String modalityName;
	/**
	 * The modality structure of the metadata
	 */
	private Structures modalityStruct;

	/**
	 * Creates a Metadata object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public Metadata(Connection dbCon) {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.metadataField = null;
		this.modalityName = null;
	}

	/**
	 * Add the attribute to a batch.
	 * 
	 * @param fieldName
	 *            name of the field
	 * @param numericValues
	 *            the numeric values of the attribute
	 * @param values
	 *            the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws MobbedException {
		addNewStructure(fieldName);
		UUID structureUUID = metadataStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < numericValues.length; i++) {
			atb.reset(UUID.randomUUID(), null, null, datasetUuid, "datasets",
					structureUUID, numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Sets the class fields of a Metadata object.
	 * 
	 * @param modalityName
	 *            the name of the modality
	 * @param datasetUuid
	 *            the UUID of the dataset
	 * @param metadataField
	 *            the name of the field that contains the metadata
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
	 *            the name of the field
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void addNewStructure(String fieldName) throws MobbedException {
		modalityStruct = Structures.retrieve(dbCon, modalityName,
				UUID.fromString(ManageDB.nullParentUuid), true);
		metadataStruct = Structures.retrieve(dbCon, metadataField,
				modalityStruct.getStructureUuid(), true);
		if (!metadataStruct.containsChild(fieldName)) {
			Structures newChild = new Structures(dbCon);
			newChild.reset(UUID.randomUUID(), fieldName,
					metadataStruct.getStructureUuid());
			newChild.save();
			metadataStruct.addChild(fieldName, newChild.getStructureUuid());
		}
	}
}
