/**
 * 
 */
package edu.utsa.testmobbed.helpers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.UUID;

import edu.utsa.mobbed.MobbedException;


/**
 * @author JCockfield
 * 
 */
public class EventTypeMaps {
	private Connection dbCon;
	private UUID eventTypeUuid;
	private UUID eventTypeEntityUuid;
	private String eventTypeEntityTable;

	public EventTypeMaps(Connection dbCon) {
		this.dbCon = dbCon;
		this.eventTypeUuid = null;
		this.eventTypeEntityUuid = null;
	}

	public void reset(UUID eventTypeUuid, UUID eventTypeEntityUuid,
			String eventTypeEntityTable) {
		this.eventTypeUuid = eventTypeUuid;
		this.eventTypeEntityUuid = eventTypeEntityUuid;
		this.eventTypeEntityTable = eventTypeEntityTable;
	}

	public int save() throws Exception {
		int insertCount = 0;
		String insertQry = "INSERT INTO EVENT_TYPE_MAPS "
				+ "(EVENT_TYPE_UUID, EVENT_TYPE_ENTITY_UUID, EVENT_TYPE_MAP_ENTITY_CLASS)" + " VALUES (?,?,?)";
		try {
			PreparedStatement pStmt = dbCon.prepareStatement(insertQry);
			pStmt.setObject(1, eventTypeUuid, Types.OTHER);
			pStmt.setObject(2, eventTypeEntityUuid, Types.OTHER);
			pStmt.setString(3, eventTypeEntityTable);
			insertCount = pStmt.executeUpdate();
		} catch (Exception ex) {
			throw new MobbedException("Could not save event type maps");
		}
		return insertCount;
	}

	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	public UUID getEventTypeEntityUuid() {
		return eventTypeEntityUuid;
	}

}
