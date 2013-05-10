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

	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws Exception {
		UUID entityUuid = null;
		String entityClass = null;
		String organizationalClass = "datasets";
		addNewStructure(fieldName);
		UUID structureUUID = metamodalityStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < numericValues.length; i++) {
			atb.reset(UUID.randomUUID(), entityUuid, entityClass, datasetUuid,
					organizationalClass, structureUUID, numericValues[i],
					values[i]);
			atb.addToBatch();
		}
	}

	private void addNewStructure(String fieldName) throws Exception {
		modalityStruct = Structures.retrieve(dbCon, modalityName,
				UUID.fromString(ManageDB.noParentUuid), true);
		metamodalityStruct = Structures.retrieve(dbCon, metadataField,
				modalityStruct.getStructureUuid(), true);
		if (!metamodalityStruct.containsChild(fieldName)) {
			Structures newStructure = new Structures(dbCon);
			newStructure.reset(UUID.randomUUID(), fieldName,
					metamodalityStruct.getStructureUuid());
			newStructure.save();
			metamodalityStruct = Structures.retrieve(dbCon, metadataField,
					modalityStruct.getStructureUuid(), true);
		}
	}

	public void reset(String modalityName, String datasetUuid,
			String metadataField) throws Exception {
		this.modalityName = modalityName;
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.metadataField = metadataField;
		atb = new Attributes(dbCon);
	}

	public void save() throws Exception {
		try {
			atb.save();
		} catch (Exception ex) {
			throw new MobbedException("Could not save metadata");
		}
	}

}
