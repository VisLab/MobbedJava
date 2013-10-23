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
	 * A hashmap that maps the event positions to the event uuids
	 */
	private HashMap<Long, UUID> eventMap;
	/**
	 * A prepared statement object that inserts events into the database
	 */
	private PreparedStatement eventstmt;
	/**
	 * Event tags
	 */
	private HashMap<Long, String[]> eventTags;
	/**
	 * A EventTypeTagModel object used to store event types
	 */
	private HashMap<String, EventTypeTags> eventTypeTagMap;
	/**
	 * Event type tags
	 */
	private HashMap<String, String[]> eventTypeTags;
	/**
	 * The UUIDs of the events
	 */
	private UUID[] eventUuids;
	/**
	 * The existing event type UUIDs in the database
	 */
	private String[] existingEventTypeUuids;
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
	 * A prepared statement object that inserts event tags into the database
	 */
	private PreparedStatement tagstmt;
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
	private static final String INSERT_EVENT_QUERY = "INSERT INTO EVENTS (EVENT_UUID, EVENT_DATASET_UUID, "
			+ " EVENT_TYPE_UUID, EVENT_START_TIME,"
			+ " EVENT_END_TIME, EVENT_PARENT_UUID, EVENT_POSITION, EVENT_CERTAINTY) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String TAG_INSERT_QUERY = "INSERT INTO TAGS (TAG_NAME, TAG_ENTITY_UUID, TAG_ENTITY_CLASS) VALUES (?,?,?)";

	/**
	 * Creates a Events object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public Events(Connection dbCon) throws Exception {
		this.dbCon = dbCon;
		this.datasetUuid = null;
		this.types = null;
		this.positions = null;
		this.startTimes = null;
		this.endTimes = null;
		this.certainties = null;
		this.uniqueTypes = null;
		atb = new Attributes(dbCon);
		eventTypeTagMap = new HashMap<String, EventTypeTags>();
		eventstmt = dbCon.prepareStatement(INSERT_EVENT_QUERY);
		tagstmt = dbCon.prepareStatement(TAG_INSERT_QUERY);
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
	 * @param original
	 *            true if the events are original, false if the events are
	 *            derived
	 * @return the UUIDs of the event types
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addEvents(boolean original) throws MobbedException {
		String[] eventTypeUuids = addNewTypes();
		if (original)
			initializeEventMap();
		eventUuids = new UUID[positions.length];
		try {
			for (int i = 0; i < positions.length; i++) {
				eventUuids[i] = UUID.randomUUID();
				eventstmt.setObject(1, eventUuids[i], Types.OTHER);
				eventstmt.setObject(2, datasetUuid, Types.OTHER);
				eventstmt
						.setObject(
								3,
								eventTypeTagMap.get(types[i].toUpperCase()).eventTypeUuid,
								Types.OTHER);
				eventstmt.setDouble(4, startTimes[i]);
				eventstmt.setDouble(5, endTimes[i]);
				eventstmt.setObject(6, eventMap.get(originalPositions[i]),
						Types.OTHER);
				eventstmt.setLong(7, positions[i]);
				eventstmt.setDouble(8, certainties[i]);
				eventstmt.addBatch();
				eventMap.put(positions[i], eventUuids[i]);
			}
			addEventTags();
		} catch (SQLException ex) {
			throw new MobbedException("Could not add the event to the batch\n"
					+ ex.getMessage());
		}
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
	 * @param originalPositions
	 *            the original positions of the events
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
	 * @param eventTags
	 *            the tags that are associated with the events
	 * @param eventTypeTags
	 *            the tags that are associated with the event types
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(String datasetUuid, double[] startTimes,
			double[] endTimes, long[] originalPositions, long[] positions,
			double[] certainties, String uniqueTypes[], String[] types,
			String[] existingEventTypeUuids, HashMap<Long, String[]> eventTags,
			HashMap<String, String[]> eventTypeTags) {
		this.datasetUuid = UUID.fromString(datasetUuid);
		this.startTimes = startTimes;
		this.endTimes = endTimes;
		this.originalPositions = originalPositions;
		this.positions = positions;
		this.certainties = certainties;
		this.uniqueTypes = uniqueTypes;
		this.types = types;
		this.existingEventTypeUuids = existingEventTypeUuids;
		this.eventTags = eventTags;
		this.eventTypeTags = eventTypeTags;
	}

	/**
	 * Saves all events as a batch. All events in the batch will be successfully
	 * written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		try {
			atb.save();
			eventstmt.executeBatch();
			tagstmt.executeBatch();
		} catch (SQLException ex) {
			throw new MobbedException("Could not save the events\n"
					+ ex.getNextException().getMessage());
		}
	}

	/**
	 * Adds the event tags to the batch.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private void addEventTags() throws MobbedException {
		try {
			for (int i = 0; i < positions.length; i++) {
				if (!ManageDB.isEmpty(eventTags.get(positions[i]))) {
					String[] tags = eventTags.get(positions[i]);
					int numTags = tags.length;
					for (int j = 0; j < numTags; j++) {
						tagstmt.setString(1, tags[j]);
						tagstmt.setObject(2, eventUuids[i], Types.OTHER);
						tagstmt.setString(3, "event_types");
						tagstmt.addBatch();
					}
				}
			}
		} catch (SQLException ex) {
			throw new MobbedException(
					"Could not add the event tags to the batch\n"
							+ ex.getMessage());
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
	private String[] addNewTypes() throws MobbedException {
		eventTypeTagMap = EventTypes.addEventTypes(dbCon,
				existingEventTypeUuids, uniqueTypes, eventTypeTags);
		String[] eventTypeUuids = EventTypes.getStringValues(eventTypeTagMap);
		return eventTypeUuids;
	}

	/**
	 * Initializes the eventMap field that maps the event positions to the event
	 * uuids.
	 */
	private void initializeEventMap() {
		eventMap = new HashMap<Long, UUID>();
		int numPos = originalPositions.length;
		for (int i = 0; i < numPos; i++) {
			eventMap.put(originalPositions[i],
					UUID.fromString(ManageDB.NO_PARENT_UUID));
		}
	}

}
