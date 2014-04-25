package edu.utsa.testmobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

public class TestGroupQueryHelper {

	public static UUID[] insertAttributes(Connection dbCon,
			UUID[] datasetUuids, UUID[] eventUuids) {
		String query = "INSERT INTO ATTRIBUTES(ATTRIBUTE_UUID, ATTRIBUTE_ENTITY_UUID, ATTRIBUTE_ENTITY_CLASS,ATTRIBUTE_ORGANIZATIONAL_UUID, ATTRIBUTE_PATH, ATTRIBUTE_NUMERIC_VALUE, ATTRIBUTE_VALUE) VALUES (?,?,?,?,?,?,?)";
		int numAttributes = 3;
		String[] attributeValues = { "attribute 1", "attribute 2", "1" };
		Double[] attributeNumericValues = { null, null, (double) 1 };
		UUID[] attributeUuids = new UUID[numAttributes];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numAttributes; i++) {
				attributeUuids[i] = UUID.randomUUID();
				st.setObject(1, attributeUuids[i]);
				st.setObject(2, eventUuids[i % 2]);
				st.setString(3, "events");
				st.setObject(4, datasetUuids[i % 2]);
				st.setString(5, "/EEG/events");
				st.setObject(6, attributeNumericValues[i]);
				st.setString(7, attributeValues[i]);
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return attributeUuids;
	}

	public static UUID[] insertDatasets(Connection dbCon) {
		String query = "INSERT INTO DATASETS(DATASET_UUID, DATASET_NAME) VALUES (?,?)";
		int numDatasets = 2;
		UUID[] datasetUuids = new UUID[numDatasets];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numDatasets; i++) {
				datasetUuids[i] = UUID.randomUUID();
				st.setObject(1, datasetUuids[i]);
				st.setString(2, "dataset " + (i + 1));
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return datasetUuids;
	}

	public static UUID[] insertEvents(Connection dbCon, UUID[] datasetUuids) {
		String query = "INSERT INTO EVENTS(EVENT_UUID, EVENT_DATASET_UUID, EVENT_START_TIME, EVENT_END_TIME) VALUES (?,?,?,?)";
		int numEvents = 2;
		UUID[] eventUuids = new UUID[numEvents];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numEvents; i++) {
				eventUuids[i] = UUID.randomUUID();
				st.setObject(1, eventUuids[i]);
				st.setObject(2, datasetUuids[i]);
				st.setDouble(3, i + 1);
				st.setDouble(4, i + 1);
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return eventUuids;
	}

	public static void insertTagEntities(Connection dbCon, UUID[] eventUuids,
			UUID[] tagUuids) {
		String query = "INSERT INTO TAG_ENTITIES(TAG_ENTITY_UUID, TAG_ENTITY_TAG_UUID, TAG_ENTITY_CLASS) VALUES(?,?,?)";
		int numTagEntities = 3;
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numTagEntities; i++) {
				st.setObject(1, eventUuids[i % 2]);
				st.setObject(2, tagUuids[i]);
				st.setString(3, "events");
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static UUID[] insertTags(Connection dbCon) {
		String query = "INSERT INTO TAGS(TAG_UUID, TAG_NAME) VALUES (?,?)";
		int numTags = 3;
		String[] tags = { "/Context/Running", "/Context/Indoors",
				"/State/Awake" };
		UUID[] tagUuids = new UUID[numTags];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numTags; i++) {
				tagUuids[i] = UUID.randomUUID();
				st.setObject(1, tagUuids[i]);
				st.setString(2, tags[i]);
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return tagUuids;
	}

	public static void populateDatabase(Connection dbCon) {
		UUID[] datasetUuids = insertDatasets(dbCon);
		UUID[] eventUuids = insertEvents(dbCon, datasetUuids);
		UUID[] tagUuids = insertTags(dbCon);
		insertTagEntities(dbCon, eventUuids, tagUuids);
		insertAttributes(dbCon, datasetUuids, eventUuids);
	}

}
