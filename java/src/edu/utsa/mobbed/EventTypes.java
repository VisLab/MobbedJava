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

	private Connection dbCon;
	private UUID eventTypeUuid;
	private String eventType;
	private String eventTypeDescription;
	private HashMap<String, UUID> etMap;

	/**
	 * Creates a EventTypes object.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 */
	public EventTypes(Connection dbCon) {
		this.dbCon = dbCon;
		this.eventTypeUuid = null;
		this.eventType = null;
		this.eventTypeDescription = null;
		etMap = new HashMap<String, UUID>();
	}

	/**
	 * Adds a event type to the hashmap.
	 * 
	 * @param eventType
	 *            - the name of the event type
	 * @param eventTypeUuid
	 *            - the UUID of the event type
	 */
	public void addEventType(String eventType, UUID eventTypeUuid) {
		etMap.put(eventType.toUpperCase(), eventTypeUuid);
	}

	/**
	 * Checks if the hashmap contains the event type.
	 * 
	 * @param eventType
	 *            - the name of the event type
	 * @return true if the hashmap contains the event type, false if otherwise
	 */
	public boolean containsEventType(String eventType) {
		return etMap.containsKey(eventType);
	}

	/**
	 * Gets the event type UUID from the hashmap.
	 * 
	 * @param eventType
	 *            - the name of the event type
	 * @return the UUID of the event type
	 */
	public UUID getEventTypeUuid(String eventType) {
		return etMap.get(eventType);
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
	 * Gets the string representation of the event type UUIDs.
	 * 
	 * @return the string representation of the event type UUIDs
	 */
	public String[] getStringValues() {
		int mapSize = etMap.size();
		String[] eventTypeUuids = new String[mapSize];
		Object[] mapObject = etMap.values().toArray();
		for (int i = 0; i < mapSize; i++)
			eventTypeUuids[i] = mapObject[i].toString();
		return eventTypeUuids;
	}

	/**
	 * Sets the class fields of a Event Type object.
	 * 
	 * @param eventType
	 *            - the name of the event type
	 * @param eventTypeDescription
	 *            - the description of the event type
	 */
	public void reset(String eventType, String eventTypeDescription) {
		this.eventTypeUuid = UUID.randomUUID();
		this.eventType = eventType;
		this.eventTypeDescription = eventTypeDescription;
	}

	/**
	 * Retrieves the event types from the database and puts them into a hashmap.
	 * 
	 * @param eventTypeUuids
	 *            - the UUIDs of the event types that exist in the database
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void retrieveMap(String[] eventTypeUuids) throws MobbedException {
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
			while (rs.next())
				etMap.put(rs.getString(1).toUpperCase(),
						UUID.fromString(rs.getString(2)));
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not retrieve the event types to put in the hashmap\n"
							+ ex.getMessage());
		}
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
