package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
	 * A HashMap that contains existing event types
	 */
	private HashMap<String, EventTypeTags> etMap;
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
		etMap = new HashMap<String, EventTypeTags>();
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
	 * Gets the event type UUID from the HashMap.
	 * 
	 * @param eventType
	 *            the name of the event type
	 * @return the UUID of the event type
	 */
	public UUID getEventTypeUuid(String eventType) {
		EventTypeTags etm = etMap.get(eventType.toUpperCase());
		return etm.getEventTypeUuid();
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

	/**
	 * Adds new event types to the database
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param eventTypeUuids
	 *            the existing event type uuids
	 * @param uniqueTypes
	 *            the names of the event types
	 * @param eventTypeTags
	 *            the tags associated with the event types
	 * @return a HashMap that contains the event types and tags
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static HashMap<String, EventTypeTags> addEventTypes(
			Connection dbCon, String[] eventTypeUuids, String uniqueTypes[],
			HashMap<String, String[]> eventTypeTags) throws MobbedException {
		HashMap<String, EventTypeTags> eventTypeMap = new HashMap<String, EventTypeTags>();
		if (!ManageDB.isEmpty(eventTypeUuids))
			eventTypeMap = retrieveEventTypeMap(dbCon, eventTypeUuids);
		for (int i = 0; i < uniqueTypes.length; i++) {
			if (!eventTypeMap.containsKey(uniqueTypes[i].toUpperCase()))
				eventTypeMap = addEventType(dbCon, uniqueTypes[i],
						eventTypeMap, eventTypeTags);
			else
				eventTypeMap = updateEventType(dbCon, uniqueTypes[i],
						eventTypeMap, eventTypeTags);
		}
		return eventTypeMap;
	}

	/**
	 * Gets the string representation of the event type UUIDs.
	 * 
	 * @param HashMap
	 *            a HashMap that contains the event type UUIDs
	 * @return the string representation of the event type UUIDs
	 */
	public static String[] getStringValues(
			HashMap<String, EventTypeTags> HashMap) {
		int numElements = HashMap.size();
		int i = 0;
		String[] eventTypeUuids = new String[numElements];
		for (EventTypeTags value : HashMap.values()) {
			eventTypeUuids[i] = value.getEventTypeUuid().toString();
			i++;
		}
		return eventTypeUuids;
	}

	/**
	 * Adds all of the tags to the database
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param eventTypeUuid
	 *            the event type UUID
	 * @param eventTypeTags
	 *            the new tags
	 * @return a HashMap containing the new tags
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, String> addAllTags(Connection dbCon,
			UUID eventTypeUuid, String[] eventTypeTags) throws MobbedException {
		String query = "INSERT INTO TAGS (TAG_NAME, TAG_ENTITY_UUID, TAG_ENTITY_CLASS) VALUES (?,?,?)";
		HashMap<String, String> eventTypeTagMap = new HashMap<String, String>();
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			for (int i = 0; i < eventTypeTags.length; i++) {
				pStmt.setString(1, eventTypeTags[i]);
				pStmt.setObject(2, eventTypeUuid, Types.OTHER);
				pStmt.setString(3, "event_types");
				pStmt.addBatch();
				eventTypeTagMap.put(eventTypeTags[i].toUpperCase(), null);
			}
			pStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add new tags to the database\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

	/**
	 * Adds a new event type and any new tags that are associated with it.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param uniqueType
	 *            the names of the event types
	 * @param eventTypeMap
	 *            a HashMap containing a mapping of the event types and the tags
	 * @param eventTypeTags
	 *            the tags associated with the event types
	 * @return
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, EventTypeTags> addEventType(
			Connection dbCon, String uniqueType,
			HashMap<String, EventTypeTags> eventTypeMap,
			HashMap<String, String[]> eventTypeTags) throws MobbedException {
		EventTypes newEventType = new EventTypes(dbCon);
		newEventType.reset(uniqueType, null);
		newEventType.save();
		UUID eventTypeUuid = newEventType.getEventTypeUuid();
		HashMap<String, String> tagMap = addAllTags(dbCon, eventTypeUuid,
				eventTypeTags.get(uniqueType));
		EventTypeTags etm = new EventTypeTags(eventTypeUuid, tagMap);
		eventTypeMap.put(uniqueType.toUpperCase(), etm);
		return eventTypeMap;
	}

	/**
	 * Adds the new tags to the database
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param tagMap
	 *            a HashMap containing the old tags
	 * @param eventTypeUuid
	 *            the event type UUID
	 * @param eventTypeTags
	 *            the new tags
	 * @return a HashMap containing the new and old tags
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, String> addNewTags(Connection dbCon,
			HashMap<String, String> tagMap, UUID eventTypeUuid,
			String[] eventTypeTags) throws MobbedException {
		String query = "INSERT INTO TAGS (TAG_NAME, TAG_ENTITY_UUID, TAG_ENTITY_CLASS) VALUES (?,?,?)";
		int numElements = eventTypeTags.length;
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			for (int i = 0; i < numElements; i++) {
				if (!tagMap.containsKey(eventTypeTags[i].toUpperCase())) {
					pStmt.setString(1, eventTypeTags[i]);
					pStmt.setObject(2, eventTypeUuid, Types.OTHER);
					pStmt.setString(3, "event_types");
					pStmt.addBatch();
					tagMap.put(eventTypeTags[i].toUpperCase(), null);
				}
			}
			pStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add new tags to the database\n"
							+ ex.getMessage());
		}
		return tagMap;
	}

	/**
	 * Retrieves the existing event types from the database that the user
	 * specifies and puts them into a HashMap.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param eventTypeUuids
	 *            the UUIDs of the event types that exist in the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, EventTypeTags> retrieveEventTypeMap(
			Connection dbCon, String[] eventTypeUuids) throws MobbedException {
		HashMap<String, EventTypeTags> eventTypeTagMap = new HashMap<String, EventTypeTags>();
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
				String eventType = rs.getString(1).toUpperCase();
				UUID eventTypeUuid = UUID.fromString(rs.getString(2));
				HashMap<String, String> eventTypeTags = retrieveEventTypeTags(
						dbCon, eventTypeUuid);
				EventTypeTags ett = new EventTypeTags(eventTypeUuid,
						eventTypeTags);
				eventTypeTagMap.put(eventType, ett);
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not retrieve the event types to put in the HashMap\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

	/**
	 * Retrieves the tags associated with the event type
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param eventTypeUuid
	 *            the event type UUID
	 * @return a HashMap containing the tags associated with the event type
	 * @throws MobbedException
	 *             if an error occurs
	 */
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
					"Could not retrieve the event types to put in the HashMap\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

	/**
	 * Updates a event type and adds any new tags that are associated with it.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param uniqueType
	 *            the names of the event types
	 * @param eventTypeMap
	 *            a HashMap containing a mapping of the event types and the tags
	 * @param eventTypeTags
	 *            the tags associated with the event types
	 * @return
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, EventTypeTags> updateEventType(
			Connection dbCon, String uniqueType,
			HashMap<String, EventTypeTags> eventTypeMap,
			HashMap<String, String[]> eventTypeTags) throws MobbedException {
		UUID eventTypeUuid = eventTypeMap.get(uniqueType.toUpperCase())
				.getEventTypeUuid();
		HashMap<String, String> tagMap = eventTypeMap.get(
				uniqueType.toUpperCase()).getTags();
		tagMap = addNewTags(dbCon, tagMap, eventTypeUuid,
				eventTypeTags.get(uniqueType));
		EventTypeTags etm = new EventTypeTags(eventTypeUuid, tagMap);
		eventTypeMap.put(uniqueType.toUpperCase(), etm);
		return eventTypeMap;
	}

}
