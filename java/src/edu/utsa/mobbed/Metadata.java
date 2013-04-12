package edu.utsa.mobbed;

import java.sql.Connection;
import java.util.UUID;

public class Metadata {

	private Attributes atb;
	private UUID datasetUuid;
	private Structures dataStruct;
	private Connection dbCon;
	private String metadataField;
	private Structures metadataStruct;
	private String modalityName;

	public Metadata(Connection dbCon) throws Exception {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.metadataField = null;
		this.modalityName = null;
	}

	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws Exception {
		addNewStructure(fieldName);
		UUID structureUUID = metadataStruct.getChildrenByName(fieldName);
		for (int i = 0; i < numericValues.length; i++) {
			atb.reset(UUID.randomUUID(), null, datasetUuid, structureUUID,
					numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	private void addNewStructure(String fieldName) throws Exception {
		metadataStruct = Structures.retrieve(dbCon, metadataField,
				dataStruct.getStructureUuid(), true);
		if (!metadataStruct.containsChild(fieldName)) {
			Structures newStructure = new Structures(dbCon);
			newStructure.reset(UUID.randomUUID(), fieldName,
					metadataStruct.getStructureUuid());
			newStructure.save();
			metadataStruct = Structures.retrieve(dbCon, metadataField,
					dataStruct.getStructureUuid(), true);
		}
	}

	public void reset(String datasetUuid, String metadataField)
			throws Exception {
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.metadataField = metadataField;
		atb = new Attributes(dbCon);
		modalityName = Structures.retrieveModalityName(dbCon,
				UUID.fromString(datasetUuid));
		dataStruct = Structures.retrieve(dbCon, modalityName, null, true);
	}

	public void save() throws Exception {
		try {
			if (atb.getBatchCount() > 0)
				atb.save();
		} catch (Exception ex) {
			throw new MobbedException("Could not save metadata");
		}
	}

}
