package edu.utsa.mobbed;

import java.sql.Connection;
import java.util.UUID;

public class Metadata {

	private Attributes atb;
	private UUID datasetUuid;
	private Structures modalityStruct;
	private Connection dbCon;
	private String metadataField;
	private Structures metamodalityStruct;
	private String modalityName;

	public Metadata(Connection dbCon) throws Exception {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.metadataField = null;
		this.modalityName = null;
	}

	/**
	 * Adds a new attribute
	 * 
	 * @param fieldName
	 * @param numericValues
	 * @param values
	 * @throws Exception
	 */
	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws Exception {
		addNewStructure(fieldName);
		UUID structureUUID = metamodalityStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < numericValues.length; i++) {
			atb.reset(UUID.randomUUID(), null, null, datasetUuid, "datasets",
					structureUUID, numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Sets class fields
	 * 
	 * @param modalityName
	 * @param datasetUuid
	 * @param metadataField
	 * @throws Exception
	 */
	public void reset(String modalityName, String datasetUuid,
			String metadataField) throws Exception {
		this.modalityName = modalityName;
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.metadataField = metadataField;
		atb = new Attributes(dbCon);
	}

	/**
	 * Saves all metadata as a batch
	 * 
	 * @throws Exception
	 */
	public void save() throws Exception {
		try {
			atb.save();
		} catch (Exception ex) {
			throw new MobbedException("Could not save metadata");
		}
	}

	/**
	 * Adds a new structure if it doesn't already exist
	 * 
	 * @param fieldName
	 * @throws Exception
	 */
	private void addNewStructure(String fieldName) throws Exception {
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
