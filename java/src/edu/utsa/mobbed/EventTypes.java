package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.UUID;

/**
 * Handler class for EVENT_TYPES table. Event types facilitate tagging and
 * association of events across multiple datasets.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class EventTypes {

	/**
	 * A connection to the database
	 */
	private Connection dbCon;
	/**
	 * A hashmap that contains existing event types
	 */
	private HashMap<String, EventTypeModel> etMap;
	/**
	 * The name of the event type
	 */
	private String eventType;
	/**
	 * The description of the event type
	 */
	private String eventTypeDescription;
	/**
	 * The UUID of the event type
	 */
	private UUID eventTypeUuid;

	/**
	 * Creates a EventTypes object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public EventTypes(Connection dbCon) {
		this.dbCon = dbCon;
		this.eventTypeUuid = null;
		this.eventType = null;
		this.eventTypeDescription = null;
		etMap = new HashMap<String, EventTypeModel>();
	}

	/**
	 * Gets the event type UUID from the event type.
	 * 
	 * @return the UUID of the event type
	 */
	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	/**
	 * Gets the event type UUID from the hashmap.
	 * 
	 * @param eventType
	 *            the name of the event type
	 * @return the UUID of the event type
	 */
	public UUID getEventTypeUuid(String eventType) {
		EventTypeModel etm = etMap.get(eventType);
		return etm.getEventTypeUuid();
	}

	/**
	 * Gets the string representation of the event type UUIDs.
	 * 
	 * @return the string representation of the event type UUIDs
	 */
	private static String[] getStringValues(
			HashMap<String, EventTypeModel> hashmap) {
		int numElements = hashmap.size();
		int i = 0;
		String[] eventTypeUuids = new String[numElements];
		for (EventTypeModel value : hashmap.values()) {
			eventTypeUuids[i] = value.getEventTypeUuid().toString();
			i++;
		}
		return eventTypeUuids;
	}

	/**
	 * Sets the class fields of a Event Type object.
	 * 
	 * @param eventType
	 *            the name of the event type
	 * @param eventTypeDescription
	 *            the description of the event type
	 */
	public void reset(String eventType, String eventTypeDescription) {
		this.eventTypeUuid = UUID.randomUUID();
		this.eventType = eventType;
		this.eventTypeDescription = eventTypeDescription;
	}

	/**
	 * Retrieves the existing event types from the database that the user
	 * specifies and puts them into a hashmap.
	 * 
	 * @param eventTypeUuids
	 *            the UUIDs of the event types that exist in the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, EventTypeModel> retrieveeventTypeMap(
			Connection dbCon, String[] eventTypeUuids) throws MobbedException {
		String eventType = null;
		UUID eventTypeUuid = null;
		HashMap<String, String> eventTypeTags;
		HashMap<String, EventTypeModel> eventTypeMap = new HashMap<String, EventTypeModel>();
		String query = "SELECT EVENT_TYPE, EVENT_TYPE_UUID FROM EVENT_TYPES WHERE EVENT_TYPE_UUID IN (";
		int numUuids = eventTypeUuids.length;
		for (int i = 0; i < numUuids - 1; i++)
			query += "?,";
		query += "?)";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			for (int i = 0; i < numUuids; i++)
				pStmt.setObject(i + 1, UUID.fromString(eventTypeUuids[i]),
						Types.OTHER);
			ResultSet rs = pStmt.executeQuery();
			while (rs.next()) {
				eventType = rs.getString(1).toUpperCase();
				eventTypeUuid = UUID.fromString(rs.getString(2));
				eventTypeTags = retrieveEventTypeTags(dbCon, eventTypeUuid);
				EventTypeModel etm = new EventTypeModel(eventTypeUuid,
						eventTypeTags);
				eventTypeMap.put(eventType, etm);
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not retrieve the event types to put in the hashmap\n"
							+ ex.getMessage());
		}
		return eventTypeMap;
	}

	/**
	 * Adds new event types to the database
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param eventTypeUuids
	 *            the existing event type uuids
	 * @param uniqueTypes
	 *            the names of the event types
	 * @return the uuids of the event types
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static String[] addNewEventTypes(Connection dbCon,
			String[] eventTypeUuids, String uniqueTypes[],
			String[] eventTypeTags) throws MobbedException {
		HashMap<String, EventTypeModel> eventTypeMap = new HashMap<String, EventTypeModel>();
		HashMap<String, String> tagMap = null;
		if (eventTypeUuids != null)
			eventTypeMap = retrieveeventTypeMap(dbCon, eventTypeUuids);
		EventTypes newEventType = new EventTypes(dbCon);
		for (int i = 0; i < uniqueTypes.length; i++) {
			if (!eventTypeMap.containsKey(uniqueTypes[i].toUpperCase())) {
				newEventType.reset(uniqueTypes[i], null);
				newEventType.save();
				tagMap = addAllTags(dbCon, newEventType.getEventTypeUuid(),
						eventTypeTags);
				EventTypeModel etm = new EventTypeModel(
						newEventType.getEventTypeUuid(), tagMap);
				eventTypeMap.put(uniqueTypes[i].toUpperCase(), etm);
			} else {
				tagMap = eventTypeMap.get(uniqueTypes[i].toUpperCase())
						.getTagMap();
				tagMap = addNewTags(dbCon, tagMap,
						newEventType.getEventTypeUuid(), eventTypeTags);
				EventTypeModel etm = new EventTypeModel(
						newEventType.getEventTypeUuid(), tagMap);
				eventTypeMap.put(uniqueTypes[i].toUpperCase(), etm);
			}
		}
		eventTypeUuids = getStringValues(eventTypeMap);
		return eventTypeUuids;
	}

	private static HashMap<String, String> addAllTags(Connection dbCon,
			UUID eventTypeUuid, String[] eventTypeTags) throws MobbedException {
		String query = "INSERT INTO TAGS (TAG_NAME, TAG_ENTITY_UUID, TAG_ENTITY_CLASS) VALUES (?,?,?)";
		HashMap<String, String> eventTypeTagMap = new HashMap<String, String>();
		int numElements = eventTypeTags.length;
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			for (int i = 0; i < numElements; i++) {
				pStmt.setString(1, eventTypeTags[i]);
				pStmt.setObject(2, eventTypeUuid, Types.OTHER);
				pStmt.setString(3, "event_types");
				pStmt.addBatch();
				eventTypeTagMap.put(eventTypeTags[i], null);
			}
			pStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add new tags to the database\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

	private static HashMap<String, String> addNewTags(Connection dbCon,
			HashMap<String, String> tagMap, UUID eventTypeUuid,
			String[] eventTypeTags) throws MobbedException {
		String query = "INSERT INTO TAGS (TAG_NAME, TAG_ENTITY_UUID, TAG_ENTITY_CLASS) VALUES (?,?,?)";
		HashMap<String, String> eventTypeTagMap = new HashMap<String, String>();
		int numElements = eventTypeTags.length;
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			for (int i = 0; i < numElements; i++) {
				if (!tagMap.containsKey(eventTypeTags[i].toUpperCase())) {
					pStmt.setString(1, eventTypeTags[i]);
					pStmt.setObject(2, eventTypeUuid, Types.OTHER);
					pStmt.setString(3, "event_types");
					pStmt.addBatch();
					eventTypeTagMap.put(eventTypeTags[i], null);
				}
			}
			pStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add new tags to the database\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

	private static HashMap<String, String> retrieveEventTypeTags(
			Connection dbCon, UUID eventTypeUuid) throws MobbedException {
		HashMap<String, String> eventTypeTagMap = new HashMap<String, String>();
		String query = "SELECT TAG_NAME FROM TAGS WHERE TAG_ENTITY_UUID = ?";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			pStmt.setObject(1, eventTypeUuid, Types.OTHER);
			ResultSet rs = pStmt.executeQuery();
			while (rs.next()) {
				eventTypeTagMap.put(rs.getString(1).toUpperCase(), null);
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not retrieve the event types to put in the hashmap\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

	/**
	 * Saves all event types as a batch. All event types in the batch will be
	 * successfully written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		String insertQry = "INSERT INTO EVENT_TYPES "
				+ "(EVENT_TYPE_UUID, EVENT_TYPE, EVENT_TYPE_DESCRIPTION)"
				+ " VALUES (?,?,?)";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(insertQry);
			pStmt.setObject(1, eventTypeUuid, Types.OTHER);
			pStmt.setString(2, eventType);
			pStmt.setString(3, eventTypeDescription);
			pStmt.executeUpdate();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save the event types\n"
					+ ex.getMessage());
		}
	}

}
