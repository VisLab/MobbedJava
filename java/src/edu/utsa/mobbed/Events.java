package edu.utsa.mobbed;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;

/**
 * Handler class for EVENTS table. Event entities have a name, a starting time,
 * and an ending time. Each event is associated with a particular target entity.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */

public class Events {

	/**
	 * A hashmap that maps the event position to the event uuid
	 */
	private static HashMap<Long, UUID> eventMap;

	/**
	 * A Attributes object used to store attributes
	 */
	private Attributes atb;
	/**
	 * The certainties of the events
	 */
	private double[] certainties;
	/**
	 * The dataset UUID of the events
	 */
	private UUID datasetUuid;
	/**
	 * A connection to the database
	 */
	private Connection dbCon;
	/**
	 * The end times of the events
	 */
	private double[] endTimes;
	/**
	 * A EventTypeTagModel object used to store event types
	 */
	private HashMap<String, EventTypeTags> eventTypeTagMap;
	/**
	 * A query that inserts events into the database
	 */
	/**
	 * Event type tags
	 */
	private String[][] eventTypeTags;
	/**
	 * The UUIDs of the events
	 */
	private UUID[] eventUuids;
	/**
	 * The existing event type UUIDs in the database
	 */
	private String[] existingEventTypeUuids;
	/**
	 * A prepared statement object that inserts events into the database
	 */
	private PreparedStatement insertStmt;
	/**
	 * The original positions of the events that have been derived
	 */
	private long[] originalPositions;
	/**
	 * The positions of the events
	 */
	private long[] positions;
	/**
	 * The start times of the events
	 */
	private double[] startTimes;
	/**
	 * The types of the events
	 */
	private String[] types;
	/**
	 * The unique types of the events
	 */
	private String[] uniqueTypes;
	/**
	 * A query that inserts events into the database
	 */
	private static final String INSERT_QUERY = "INSERT INTO EVENTS (EVENT_UUID, EVENT_DATASET_UUID, "
			+ " EVENT_TYPE_UUID, EVENT_START_TIME,"
			+ " EVENT_END_TIME, EVENT_PARENT_UUID, EVENT_POSITION, EVENT_CERTAINTY) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

	/**
	 * Creates a Events object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public Events(Connection dbCon) {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.types = null;
		this.positions = null;
		this.startTimes = null;
		this.endTimes = null;
		this.certainties = null;
		this.uniqueTypes = null;
	}

	/**
	 * Add the attribute to a batch.
	 * 
	 * @param path
	 *            the path of the attribute
	 * @param numericValues
	 *            the numeric values of the attribute
	 * @param values
	 *            the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addAttribute(String path, Double[] numericValues,
			String[] values) throws MobbedException {
		for (int i = 0; i < types.length; i++) {
			atb.reset(UUID.randomUUID(), eventUuids[i], "events", datasetUuid,
					path, numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Adds the events to a batch. If working with the EEG modality the original
	 * events should be stored prior to storing the events that are derived from
	 * them.
	 * 
	 * @return the UUIDs of the events that were stored
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addEvents(boolean original) throws MobbedException {
		if (original)
			initializeEventMap();
		eventUuids = new UUID[positions.length];
		try {
			insertStmt = dbCon.prepareStatement(INSERT_QUERY);
			for (int i = 0; i < positions.length; i++) {
				eventUuids[i] = UUID.randomUUID();
				insertStmt.setObject(1, eventUuids[i], Types.OTHER);
				insertStmt.setObject(2, datasetUuid, Types.OTHER);
				insertStmt
						.setObject(
								3,
								eventTypeTagMap.get(types[i].toUpperCase()).eventTypeUuid,
								Types.OTHER);
				insertStmt.setDouble(4, startTimes[i]);
				insertStmt.setDouble(5, endTimes[i]);
				insertStmt.setObject(6, eventMap.get(originalPositions[i]),
						Types.OTHER);
				insertStmt.setLong(7, positions[i]);
				insertStmt.setDouble(8, certainties[i]);
				insertStmt.addBatch();
				eventMap.put(positions[i], eventUuids[i]);
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not add the event to the batch\n"
					+ ex.getMessage());
		}
	}

	/**
	 * Initializes the eventMap
	 */
	public void initializeEventMap() {
		eventMap = new HashMap<Long, UUID>();
		int numPos = originalPositions.length;
		for (int i = 0; i < numPos; i++) {
			eventMap.put(originalPositions[i],
					UUID.fromString(ManageDB.NO_PARENT_UUID));
		}
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
		String[] eventTypeUuids = null;
		eventTypeTagMap = EventTypes.addNewEventTypes(dbCon,
				existingEventTypeUuids, uniqueTypes, eventTypeTags);
		eventTypeUuids = EventTypes.getStringValues(eventTypeTagMap);
		return eventTypeUuids;
	}

	/**
	 * Sets class fields of a Events object.
	 * 
	 * @param datasetUuid
	 *            the UUID of the dataset associated with the events
	 * @param startTimes
	 *            the start times of the events
	 * @param endTimes
	 *            the end times of the events
	 * @param eventParentUuids
	 *            the uuids of the parent events
	 * @param positions
	 *            the positions of the events
	 * @param certainties
	 *            the certainties of the events
	 * @param uniqueTypes
	 *            the unique event types
	 * @param types
	 *            all of the event types
	 * @param existingEventTypeUuids
	 *            the UUIDs of the event types that will be reused
	 * @param eventTypeTags
	 *            the tags that are associated with the event types
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(String datasetUuid, double[] startTimes,
			double[] endTimes, long[] originalPositions, long[] positions,
			double[] certainties, String uniqueTypes[], String[] types,
			String[] existingEventTypeUuids, String[][] eventTypeTags)
			throws MobbedException {
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.startTimes = startTimes;
		this.endTimes = endTimes;
		this.originalPositions = originalPositions;
		this.positions = positions;
		this.certainties = certainties;
		this.uniqueTypes = uniqueTypes;
		this.types = types;
		this.existingEventTypeUuids = existingEventTypeUuids;
		this.eventTypeTags = eventTypeTags;
		atb = new Attributes(dbCon);
		eventTypeTagMap = new HashMap<String, EventTypeTags>();
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
			throw new MobbedException("Could not save the events\n"
					+ ex.getNextException().getMessage());
		}
	}

}
