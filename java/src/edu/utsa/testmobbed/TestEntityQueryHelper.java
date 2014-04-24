package edu.utsa.testmobbed;

import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

import edu.utsa.mobbed.Datadefs;

public class TestEntityQueryHelper {

	public static UUID[] insertContacts(Connection dbCon) {
		String query = "INSERT INTO CONTACTS(CONTACT_UUID, CONTACT_FIRST_NAME, CONTACT_LAST_NAME) VALUES (?,?,?)";
		String[] firstNames = { "John", "Jane", "Tim" };
		String[] lastNames = { "Doe", "Doe", "Smith" };
		int numContacts = 3;
		UUID[] contactUuids = new UUID[numContacts];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numContacts; i++) {
				contactUuids[i] = UUID.randomUUID();
				st.setObject(1, contactUuids[i]);
				st.setString(2, firstNames[i]);
				st.setString(3, lastNames[i]);
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return contactUuids;
	}

	public static long[] insertDatasetOids(Connection dbCon, UUID[] datasetUuids) {
		int numDatasets = datasetUuids.length;
		long[] datasetOids = new long[datasetUuids.length];
		try {
			String filename = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/blob.txt")
							.getPath(), "UTF-8");
			for (int i = 0; i < numDatasets; i++) {
				datasetOids[i] = Datadefs.storeBlob(dbCon, filename,
						datasetUuids[i].toString(), true);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return datasetOids;
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

	public static UUID[] insertElements(Connection dbCon) {
		String query = "INSERT INTO ELEMENTS(ELEMENT_UUID, ELEMENT_LABEL,  ELEMENT_POSITION) VALUES (?,?,?)";
		String[] labels = { "e1", "e2", "e3" };
		int numElements = 3;
		UUID[] elementUuids = new UUID[numElements];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numElements; i++) {
				elementUuids[i] = UUID.randomUUID();
				st.setObject(1, elementUuids[i]);
				st.setString(2, labels[i]);
				st.setLong(3, i + 1);
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return elementUuids;
	}

	public static UUID[] insertEvents(Connection dbCon) {
		String query = "INSERT INTO EVENTS(EVENT_UUID,  EVENT_START_TIME, EVENT_END_TIME, EVENT_POSITION) VALUES (?,?,?,?)";
		int numEvents = 3;
		UUID[] eventUuids = new UUID[numEvents];
		try {
			PreparedStatement st = dbCon.prepareStatement(query);
			for (int i = 0; i < numEvents; i++) {
				eventUuids[i] = UUID.randomUUID();
				st.setObject(1, eventUuids[i]);
				st.setDouble(2, i + 1);
				st.setDouble(3, i + 1);
				st.setLong(4, i + 1);
				st.execute();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return eventUuids;
	}

}
