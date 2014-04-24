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

	private static HashMap<String, UUID> tagMap;

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
	 * @param uniqueEventTypes
	 *            the names of the event types
	 * @param eventTypeTags
	 *            the tags associated with the event types
	 * @return a HashMap that contains the event types and tags
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static HashMap<String, EventTypeTags> addEventTypes(
			Connection dbCon, String[] eventTypeUuids,
			String[] eventTypeDescriptions, String uniqueEventTypes[],
			HashMap<String, String[]> eventTypeTags,
			HashMap<String, UUID> originalTagMap) throws MobbedException {
		tagMap = originalTagMap;
		HashMap<String, EventTypeTags> eventTypeMap = new HashMap<String, EventTypeTags>();
		if (!ManageDB.isEmpty(eventTypeUuids))
			eventTypeMap = retrieveEventTypeMap(dbCon, eventTypeUuids);
		for (int i = 0; i < uniqueEventTypes.length; i++) {
			if (!eventTypeMap.containsKey(uniqueEventTypes[i].toUpperCase())) {
				EventTypes newEventType = new EventTypes(dbCon);
				newEventType.reset(uniqueEventTypes[i],
						eventTypeDescriptions[i]);
				newEventType.save();
				HashMap<String, UUID> eventTypeTagMap = new HashMap<String, UUID>();
				if (!ManageDB.isEmpty(eventTypeTags.get(uniqueEventTypes[i])))
					eventTypeTagMap = addAllTags(dbCon,
							newEventType.getEventTypeUuid(),
							eventTypeTags.get(uniqueEventTypes[i]));
				EventTypeTags etm = new EventTypeTags(
						newEventType.getEventTypeUuid(), eventTypeTagMap);
				eventTypeMap.put(uniqueEventTypes[i].toUpperCase(), etm);
			} else if (!ManageDB
					.isEmpty(eventTypeTags.get(uniqueEventTypes[i]))) {
				HashMap<String, UUID> eventTypeTagMap = addEventTypeTags(dbCon,
						eventTypeMap.get(uniqueEventTypes[i].toUpperCase())
								.getTags(),
						eventTypeMap.get(uniqueEventTypes[i].toUpperCase())
								.getEventTypeUuid(),
						eventTypeTags.get(uniqueEventTypes[i]));
				EventTypeTags etm = new EventTypeTags(eventTypeMap.get(
						uniqueEventTypes[i].toUpperCase()).getEventTypeUuid(),
						eventTypeTagMap);
				eventTypeMap.put(uniqueEventTypes[i].toUpperCase(), etm);
			}
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
	private static HashMap<String, UUID> addAllTags(Connection dbCon,
			UUID eventTypeUuid, String[] eventTypeTags) throws MobbedException {
		String tagQry = "INSERT INTO TAGS (TAG_UUID, TAG_NAMES) VALUES (?,?)";
		String tagEntityQry = "INSERT INTO TAG_ENTITIES (TAG_ENTITY_UUID, TAG_ENTITY_TAG_UUID,TAG_ENTITY_CLASS) VALUES (?,?,?)";
		HashMap<String, UUID> eventTypeTagMap = new HashMap<String, UUID>();
		try {
			PreparedStatement tagSmt = dbCon.prepareStatement(tagQry);
			PreparedStatement tagEntitySmt = dbCon
					.prepareStatement(tagEntityQry);
			for (int i = 0; i < eventTypeTags.length; i++) {
				if (!tagMap.containsKey(eventTypeTags[i].toUpperCase())) {
					UUID tagUuid = UUID.randomUUID();
					tagSmt.setObject(1, tagUuid, Types.OTHER);
					tagSmt.setString(2, eventTypeTags[i]);
					tagMap.put(eventTypeTags[i].toUpperCase(), tagUuid);
					eventTypeTagMap
							.put(eventTypeTags[i].toUpperCase(), tagUuid);
				}
				tagEntitySmt.setObject(1, eventTypeUuid, Types.OTHER);
				tagEntitySmt
						.setObject(2,
								tagMap.get(eventTypeTags[i].toUpperCase()),
								Types.OTHER);
				tagEntitySmt.setString(3, "event_types");
				tagSmt.addBatch();
				tagEntitySmt.addBatch();
			}
			tagSmt.executeBatch();
			tagEntitySmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add new tags to the database\n"
							+ ex.getMessage());
		}
		return tagMap;
	}

	/**
	 * Adds the new tags to the database
	 * 
	 * @param dbCon
	 *            a connection to the database
	 * @param typeTagMap
	 *            a HashMap containing the old tags
	 * @param eventTypeUuid
	 *            the event type UUID
	 * @param eventTypeTags
	 *            the new tags
	 * @return a HashMap containing the new and old tags
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private static HashMap<String, UUID> addEventTypeTags(Connection dbCon,
			HashMap<String, UUID> typeTagMap, UUID eventTypeUuid,
			String[] eventTypeTags) throws MobbedException {
		String tagQry = "INSERT INTO TAGS (TAG_UUID, TAG_NAME) VALUES (?,?)";
		String tagEntityQry = "INSERT INTO TAG_ENTITIES (TAG_ENTITY_UUID,TAG_ENTITY_TAG_UUID,TAG_ENTITY_CLASS) VALUES (?,?,?)";
		int numElements = eventTypeTags.length;
		try {
			PreparedStatement tagSmt = dbCon.prepareStatement(tagQry);
			PreparedStatement tagEntitySmt = dbCon
					.prepareStatement(tagEntityQry);
			for (int i = 0; i < numElements; i++) {
				if (!typeTagMap.containsKey(eventTypeTags[i].toUpperCase())) {
					if (!tagMap.containsKey(eventTypeTags[i].toUpperCase())) {
						UUID tagUuid = UUID.randomUUID();
						tagSmt.setObject(1, tagUuid, Types.OTHER);
						tagSmt.setString(2, eventTypeTags[i]);
						tagMap.put(eventTypeTags[i].toUpperCase(), tagUuid);
						typeTagMap.put(eventTypeTags[i].toUpperCase(), tagUuid);
						tagSmt.addBatch();
					}
					tagEntitySmt.setObject(1, eventTypeUuid, Types.OTHER);
					tagEntitySmt.setObject(2,
							tagMap.get(eventTypeTags[i].toUpperCase()),
							Types.OTHER);
					tagEntitySmt.setString(3, "event_types");
					tagEntitySmt.addBatch();
				}
			}
			tagSmt.executeBatch();
			tagEntitySmt.executeBatch();
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
				HashMap<String, UUID> eventTypeTags = retrieveEventTypeTags(
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
	private static HashMap<String, UUID> retrieveEventTypeTags(
			Connection dbCon, UUID eventTypeUuid) throws MobbedException {
		HashMap<String, UUID> eventTypeTagMap = new HashMap<String, UUID>();
		String query = "SELECT TAGS.TAG_NAME, TAGS.TAG_UUID FROM TAGS INNER JOIN TAG_ENTITIES ON TAGS.TAG_UUID = TAG_ENTITIES.TAG_ENTITY_TAG_UUID WHERE TAG_ENTITY_UUID = ?";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(query);
			pStmt.setObject(1, eventTypeUuid, Types.OTHER);
			ResultSet rs = pStmt.executeQuery();
			while (rs.next()) {
				eventTypeTagMap.put(rs.getString(1).toUpperCase(),
						UUID.fromString(rs.getString(2)));
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not retrieve the event types to put in the HashMap\n"
							+ ex.getMessage());
		}
		return eventTypeTagMap;
	}

}
