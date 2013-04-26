package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashMap;
import java.util.UUID;

public class EventTypes {

	private Connection dbCon;
	private UUID eventTypeUuid;
	private String eventType;
	private String description;
	private HashMap<String, UUID> name2UuidMap;
	private HashMap<String, String> uuid2NameMap;

	public EventTypes(Connection dbCon) {
		this.dbCon = dbCon;
		this.eventTypeUuid = null;
		this.eventType = null;
		this.description = null;
		name2UuidMap = new HashMap<String, UUID>();
		uuid2NameMap = new HashMap<String, String>();
	}

	public void reset(String eventType, String description) {
		this.eventTypeUuid = UUID.randomUUID();
		this.eventType = eventType;
		this.description = description;
	}

	public void retrieveName2UuidMap(String[] existingEventTypes)
			throws Exception {
		String selQry = "SELECT * FROM EVENT_TYPES WHERE EVENT_TYPE_UUID IN (";
		int numUuids = existingEventTypes.length;
		for (int i = 0; i < numUuids; i++) {
			if (i == numUuids - 1)
				selQry += "'" + existingEventTypes[i] + "')";
			else
				selQry += "'" + existingEventTypes[i] + "',";
		}
		int rowCount = 0;
		PreparedStatement pStmt = dbCon.prepareStatement(selQry);
		ResultSet rs = pStmt.executeQuery();
		while (rs.next()) {
			rowCount++;
			name2UuidMap.put(rs.getString("EVENT_TYPE").toUpperCase(),
					UUID.fromString(rs.getString("EVENT_TYPE_UUID")));
		}
		if (rowCount != existingEventTypes.length)
			throw new MobbedException("Event type(s) does not exist");
	}

	public void retrieveUuid2NameMap(UUID entityUuid) throws Exception {
		String selQry = "SELECT * FROM EVENT_TYPES WHERE EVENT_TYPE_UUID IN "
				+ "(SELECT EVENT_TYPE_UUID FROM EVENTS WHERE EVENT_ENTITY_UUID = ?)";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(selQry);
			pStmt.setObject(1, entityUuid, Types.OTHER);
			ResultSet rs = pStmt.executeQuery();
			while (rs.next())
				uuid2NameMap.put(rs.getString("EVENT_TYPE_UUID"),
						rs.getString("EVENT_TYPE"));
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve event types");
		}
	}

	public void addToHashMap() {
		name2UuidMap.put(eventType.toUpperCase(), eventTypeUuid);
	}

	public UUID[] getUUIDValues() {
		int mapSize = name2UuidMap.size();
		UUID[] eventTypeUuids = new UUID[mapSize];
		Object[] mapObject = name2UuidMap.values().toArray();
		for (int i = 0; i < mapSize; i++)
			eventTypeUuids[i] = (UUID) mapObject[i];
		return eventTypeUuids;
	}

	public String[] getStringValues() {
		int mapSize = name2UuidMap.size();
		String[] eventTypeUuids = new String[mapSize];
		Object[] mapObject = name2UuidMap.values().toArray();
		for (int i = 0; i < mapSize; i++)
			eventTypeUuids[i] = mapObject[i].toString();
		return eventTypeUuids;
	}

	public boolean typeExists(String eventType) {
		return name2UuidMap.containsKey(eventType);
	}

	public UUID getTypeUuid(String eventType) {
		return name2UuidMap.get(eventType);
	}

	public String getTypeName(String typeUuid) {
		return uuid2NameMap.get(typeUuid);
	}

	public int save() throws Exception {
		int insertCount = 0;
		String insertQry = "INSERT INTO EVENT_TYPES "
				+ "(EVENT_TYPE_UUID, EVENT_TYPE, EVENT_TYPE_DESCRIPTION)"
				+ " VALUES (?,?,?)";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(insertQry);
			pStmt.setObject(1, eventTypeUuid, Types.OTHER);
			pStmt.setString(2, eventType);
			pStmt.setString(3, description);
			insertCount = pStmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not save event types");
		}
		return insertCount;
	}

	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	public String getEventType() {
		return eventType;
	}

	public String getDescription() {
		return description;
	}

	public HashMap<String, UUID> getname2UuidMap() {
		return name2UuidMap;
	}

	public HashMap<String, String> getuuid2NameMap() {
		return uuid2NameMap;
	}
}
