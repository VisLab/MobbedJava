package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
	 * Creates a EventTypes object
	 * 
	 * @param dbCon
	 */
	public EventTypes(Connection dbCon) {
		this.dbCon = dbCon;
		this.eventTypeUuid = null;
		this.eventType = null;
		this.eventTypeDescription = null;
		etMap = new HashMap<String, UUID>();
	}

	/**
	 * Add a event type to the hashmap
	 */
	public void addEventType(String eventType, UUID eventTypeUuid) {
		etMap.put(eventType.toUpperCase(), eventTypeUuid);
	}

	/**
	 * Checks if the hashmap contains the event type
	 * 
	 * @param eventType
	 * @return
	 */
	public boolean containsEventType(String eventType) {
		return etMap.containsKey(eventType);
	}

	/**
	 * Get the event type uuid from the hashmap
	 * 
	 * @param eventType
	 * @return
	 */
	public UUID getEventTypeUuid(String eventType) {
		return etMap.get(eventType);
	}

	/**
	 * Get the event type uuid from the event type object
	 * 
	 * @param eventType
	 * @return
	 */
	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	/**
	 * Converts the event type uuids to strings and puts them in a array
	 * 
	 * @return
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
	 * Sets class fields
	 * 
	 * @param eventType
	 * @param eventTypeDescription
	 */
	public void reset(String eventType, String eventTypeDescription) {
		this.eventTypeUuid = UUID.randomUUID();
		this.eventType = eventType;
		this.eventTypeDescription = eventTypeDescription;
	}

	/**
	 * Retrieves the event types from the database and puts them into a hashmap
	 * 
	 * @param eventTypeUuids
	 * @throws Exception
	 */
	public void retrieveMap(String[] eventTypeUuids) throws Exception {
		String selQry = "SELECT EVENT_TYPE, EVENT_TYPE_UUID FROM EVENT_TYPES WHERE EVENT_TYPE_UUID IN (";
		int numUuids = eventTypeUuids.length;
		for (int i = 0; i < numUuids - 1; i++)
			selQry += "?,";
		selQry += "?)";
		PreparedStatement pStmt = dbCon.prepareStatement(selQry);
		for (int i = 0; i < numUuids; i++)
			pStmt.setObject(i + 1, UUID.fromString(eventTypeUuids[i]),
					Types.OTHER);
		ResultSet rs = pStmt.executeQuery();
		while (rs.next())
			etMap.put(rs.getString(1).toUpperCase(),
					UUID.fromString(rs.getString(2)));
	}

	/**
	 * Saves a event type to the database
	 * 
	 * @throws Exception
	 */
	public void save() throws Exception {
		String insertQry = "INSERT INTO EVENT_TYPES "
				+ "(EVENT_TYPE_UUID, EVENT_TYPE, EVENT_TYPE_DESCRIPTION)"
				+ " VALUES (?,?,?)";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(insertQry);
			pStmt.setObject(1, eventTypeUuid, Types.OTHER);
			pStmt.setString(2, eventType);
			pStmt.setString(3, eventTypeDescription);
			pStmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not save event types");
		}
	}

}
