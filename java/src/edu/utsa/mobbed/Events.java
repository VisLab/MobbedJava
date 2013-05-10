package edu.utsa.mobbed;

import java.sql.*;
import java.util.UUID;

/**
 * Handler class for EVENTS table. Event entities have a name, a starting time,
 * and an ending time. Each event is associated with a particular target entity.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */

public class Events {

	private Attributes atb;
	private double[] certainties;
	private UUID datasetUuid;
	private Structures modalityStruct;
	private Connection dbCon;
	private String eventField;
	private Structures eventStruct;
	private UUID[] eventUuids;
	private EventTypes evType;
	private String[] existingUuids;
	private PreparedStatement insertStmt;
	private String modalityName;
	private long[] positions;
	private double[] startTimes;
	private double[] endTimes;
	private String[] types;
	private String[] uniqueTypes;
	private String[] parentUuids;
	private static final String insertQry = "INSERT INTO EVENTS (EVENT_UUID, EVENT_ENTITY_UUID, "
			+ "EVENT_ENTITY_CLASS, EVENT_TYPE_UUID, EVENT_PARENT_UUID, EVENT_POSITION, EVENT_START_TIME,"
			+ " EVENT_END_TIME, EVENT_CERTAINTY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	/**
	 * Creates a Events object
	 * 
	 * @param dbCon
	 *            connection to the database
	 * @param datasetUuid
	 *            UUID of the entity
	 */
	public Events(Connection dbCon) throws Exception {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.modalityName = null;
		this.types = null;
		this.positions = null;
		this.startTimes = null;
		this.endTimes = null;
		this.certainties = null;
		this.uniqueTypes = null;
		this.modalityStruct = null;
	}

	/**
	 * Add the attribute to a batch
	 * 
	 * @param fieldName
	 * @param numericFieldValues
	 * @param fieldValues
	 * @throws Exception
	 */
	public void addAttribute(String fieldName, Double[] numericFieldValues,
			String[] fieldValues) throws Exception {
		addNewStructure(fieldName);
		UUID structureUUID = eventStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < types.length; i++) {
			atb.reset(UUID.randomUUID(), eventUuids[i], "events", datasetUuid,
					"datasets", structureUUID, numericFieldValues[i],
					fieldValues[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Add the events to a batch
	 * 
	 * @throws Exception
	 */
	public String[] addEvents() throws Exception {
		insertStmt = dbCon.prepareStatement(insertQry);
		addNewTypes();
		String entityClass = "datasets";
		eventUuids = new UUID[types.length];
		String[] stringEventUuids = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			eventUuids[i] = UUID.randomUUID();
			stringEventUuids[i] = eventUuids[i].toString();
			insertStmt.setObject(1, eventUuids[i], Types.OTHER);
			insertStmt.setObject(2, datasetUuid, Types.OTHER);
			insertStmt.setString(3, entityClass);
			insertStmt.setObject(4,
					evType.getEventTypeUuid(types[i].toUpperCase()),
					Types.OTHER);
			insertStmt.setObject(5, parentUuids[i], Types.OTHER);
			insertStmt.setLong(6, positions[i]);
			insertStmt.setDouble(7, startTimes[i]);
			insertStmt.setDouble(8, endTimes[i]);
			insertStmt.setDouble(9, certainties[i]);
			insertStmt.addBatch();
		}
		return stringEventUuids;
	}

	/**
	 * Add new event types if they don't exist
	 * 
	 * @return
	 * @throws Exception
	 */
	public String[] addNewTypes() throws Exception {
		if (existingUuids != null)
			evType.retrieveMap(existingUuids);
		EventTypes newEventType = new EventTypes(dbCon);
		for (int i = 0; i < uniqueTypes.length; i++) {
			if (!evType.containsEventType(uniqueTypes[i].toUpperCase())) {
				newEventType.reset(uniqueTypes[i], null);
				newEventType.save();
				evType.addEventType(uniqueTypes[i].toUpperCase(),
						newEventType.getEventTypeUuid());
			}
		}
		return evType.getStringValues();
	}

	/**
	 * Sets class fields
	 * 
	 * @param modalityName
	 * @param datasetUuid
	 * @param eventField
	 * @param uniqueTypes
	 * @param types
	 * @param positions
	 * @param startTimes
	 * @param endTimes
	 * @param certainties
	 * @param existingUuids
	 * @param parentUuids
	 * @throws Exception
	 */
	public void reset(String modalityName, String datasetUuid,
			String eventField, String uniqueTypes[], String[] types,
			long[] positions, double[] startTimes, double[] endTimes,
			double[] certainties, String[] existingUuids, String[] parentUuids)
			throws Exception {
		this.modalityName = modalityName;
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.eventField = eventField;
		this.types = types;
		this.positions = positions;
		this.startTimes = startTimes;
		this.endTimes = endTimes;
		this.certainties = certainties;
		this.uniqueTypes = uniqueTypes;
		this.existingUuids = existingUuids;
		this.parentUuids = parentUuids;
		atb = new Attributes(dbCon);
		evType = new EventTypes(dbCon);
	}

	/**
	 * Saves all events as a batch
	 * 
	 * @throws Exception
	 */
	public void save() throws Exception {
		try {
			insertStmt.executeBatch();
			atb.save();
		} catch (Exception ex) {
			throw new MobbedException("Could not save events");
		}
	}

	/**
	 * Adds a new structure if it doesn't exist
	 * 
	 * @param fieldName
	 * @throws Exception
	 */
	private void addNewStructure(String fieldName) throws Exception {
		modalityStruct = Structures.retrieve(dbCon, modalityName,
				UUID.fromString(ManageDB.noParentUuid), false);
		eventStruct = Structures.retrieve(dbCon, eventField,
				modalityStruct.getStructureUuid(), true);
		if (!eventStruct.containsChild(fieldName)) {
			Structures newChild = new Structures(dbCon);
			newChild.reset(UUID.randomUUID(), fieldName,
					eventStruct.getStructureUuid());
			newChild.save();
			eventStruct.addChild(fieldName, newChild.getStructureUuid());
		}
	}

}
