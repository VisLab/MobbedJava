package edu.utsa.mobbed;

import java.sql.*;
import java.util.UUID;

/**
 * Handler class for EVENTS table. An Event is a recorded activity generated by
 * system or from the subject of the experiment. Class contains functions to
 * store, retrieve, delete or making any other queries on the table. A record in
 * the table is uniquely identified by EVENT_UUID.
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */

public class Events {

	private Attributes atb;
	private double[] certainties;
	private UUID datasetUuid;
	private Structures dataStruct;
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
	 * create an Events object
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
		this.dataStruct = null;
	}

	public void addAttribute(String fieldName, Double[] numericFieldValues,
			String[] fieldValues) throws Exception {
		String entityClass = "events";
		String organizationalClass = "datasets";
		addNewStructure(fieldName);
		UUID structureUUID = eventStruct.getChildrenByName(fieldName);
		for (int i = 0; i < types.length; i++) {
			atb.reset(UUID.randomUUID(), eventUuids[i], entityClass,
					datasetUuid, organizationalClass, structureUUID,
					numericFieldValues[i], fieldValues[i]);
			atb.addToBatch();
		}
	}

	/**
	 * add Events to the database
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
			insertStmt.setObject(4, evType.getTypeUuid(types[i].toUpperCase()),
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
	 * Add a new structure to the elementStruct
	 * 
	 * @param fieldName
	 *            name of the structure element to be added
	 * @param handler
	 *            - code for the integer
	 * @throws Exception
	 */
	private void addNewStructure(String fieldName) throws Exception {
		eventStruct = Structures.retrieve(dbCon, eventField,
				dataStruct.getStructureUuid(), true);
		if (!eventStruct.containsChild(fieldName)) {
			Structures newStructure = new Structures(dbCon);
			newStructure.reset(UUID.randomUUID(), fieldName,
					eventStruct.getStructureUuid());
			newStructure.save();
			eventStruct = Structures.retrieve(dbCon, "event",
					dataStruct.getStructureUuid(), true);
		}
	}

	public String[] addNewTypes() throws Exception {
		if (existingUuids != null)
			evType.retrieveName2UuidMap(existingUuids);
		for (int i = 0; i < uniqueTypes.length; i++) {
			if (!evType.typeExists(uniqueTypes[i].toUpperCase())) {
				evType.reset(uniqueTypes[i], null);
				evType.save();
				evType.addToHashMap();
			}
		}
		return evType.getStringValues();
	}

	/**
	 * Create a new set of events to be added to the database
	 * 
	 * @param eventField
	 * @param uniqueTypes
	 * @param types
	 * @param positions
	 * @param startTimes
	 * @param endTimes
	 * @param otherFields
	 * @throws Exception
	 */
	public void reset(String datasetUuid, String eventField,
			String[] defaultFields, String uniqueTypes[], String[] types,
			long[] positions, double[] startTimes, double[] endTimes,
			double[] certainties, String[] existingUuids, String[] parentUuids)
			throws Exception {
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
		modalityName = Structures.retrieveModalityName(dbCon,
				UUID.fromString(datasetUuid));
		dataStruct = Structures.retrieve(dbCon, modalityName, null, false);
	}

	/**
	 * Saves events in batch
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

}
