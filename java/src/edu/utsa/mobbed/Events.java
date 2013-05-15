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
	 * Creates a Events object.
	 * 
	 * @param dbCon
	 *            - a connection to the database
	 */
	public Events(Connection dbCon) {
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
	 * Add the attribute to a batch.
	 * 
	 * @param fieldName
	 *            - the field name of the attribute
	 * @param numericValues
	 *            - the numeric values of the attribute
	 * @param values
	 *            - the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addAttribute(String fieldName, Double[] numericValues,
			String[] values) throws MobbedException {
		addNewStructure(fieldName);
		UUID structureUUID = eventStruct.getChildStructUuid(fieldName);
		for (int i = 0; i < types.length; i++) {
			atb.reset(UUID.randomUUID(), eventUuids[i], "events", datasetUuid,
					"datasets", structureUUID, numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Adds the events to a batch. The original events should be stored prior to
	 * storing the events that are derived from them.
	 * 
	 * @return the UUIDs of the events that were stored
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addEvents() throws MobbedException {
		addNewTypes();
		String entityClass = "datasets";
		eventUuids = new UUID[types.length];
		String[] stringEventUuids = new String[types.length];
		try {
			insertStmt = dbCon.prepareStatement(insertQry);
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
		} catch (SQLException ex) {
			throw new MobbedException("Could not add events to the batch\n"
					+ ex.getNextException().getMessage());
		}
		return stringEventUuids;
	}

	/**
	 * Adds a new event type if it does not already exist. Event types should be
	 * reused when storing datasets that have event types with the same meaning.
	 * 
	 * @return the UUIDs of the event types
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addNewTypes() throws MobbedException {
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
	 * Sets class fields of a Events object.
	 * 
	 * @param modalityName
	 *            - the modality name associated with the events
	 * @param datasetUuid
	 *            - the UUID of the dataset associated with the events
	 * @param eventField
	 *            - the name of the field that contains the events
	 * @param uniqueTypes
	 *            - the unique event types
	 * @param types
	 *            - all of the event types
	 * @param positions
	 *            - the positions of the events
	 * @param startTimes
	 *            - the start times of the events
	 * @param endTimes
	 *            - the end times of the events
	 * @param certainties
	 *            - the certainties of the events
	 * @param existingUuids
	 *            - the UUIDs of the event types that will be reused
	 * @param parentUuids
	 *            - the UUIDs of the original events in which these events were
	 *            derived from
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(String modalityName, String datasetUuid,
			String eventField, String uniqueTypes[], String[] types,
			long[] positions, double[] startTimes, double[] endTimes,
			double[] certainties, String[] existingUuids, String[] parentUuids)
			throws MobbedException {
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
	 * Saves all events as a batch. All events in the batch will be successfully
	 * written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		atb.save();
		try {
			insertStmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save events\n"
					+ ex.getNextException().getMessage());
		}
	}

	/**
	 * Adds a field to the structures table if it does not already exist.
	 * 
	 * @param fieldName
	 *            - the name of the field
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void addNewStructure(String fieldName) throws MobbedException {
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
