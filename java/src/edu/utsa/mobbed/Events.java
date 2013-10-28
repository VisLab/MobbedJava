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
	 * The eventCertainties of the events
	 */
	private double[] eventCertainties;
	/**
	 * The dataset UUID of the events
	 */
	private UUID eventDatasetUuid;
	/**
	 * A connection to the database
	 */
	private Connection dbCon;
	/**
	 * The end times of the events
	 */
	private double[] eventEndTimes;
	/**
	 * A HashMap that maps the event eventPositions to the event uuids
	 */
	private HashMap<Long, UUID> eventMap;
	/**
	 * A prepared statement object that inserts events into the database
	 */
	private PreparedStatement eventstmt;
	/**
	 * A HashMap that maps tags to events
	 */
	private HashMap<Long, String[]> eventTags;
	/**
	 * A EventTypeTagModel object used to store event eventTypes
	 */
	private HashMap<String, EventTypeTags> eventTypeTagMap;
	/**
	 * A HashMap that maps tags to event types
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
	 * The original eventPositions of the events that have been derived
	 */
	private long[] originalEventPositions;
	/**
	 * The eventPositions of the events
	 */
	private long[] eventPositions;
	/**
	 * The start times of the events
	 */
	private double[] eventStartTimes;
	/**
	 * A prepared statement object that inserts event tags into the database
	 */
	private PreparedStatement tagstmt;
	/**
	 * The descriptions of the event types
	 */
	private String[] eventTypeDescriptions;
	/**
	 * The event types
	 */
	private String[] eventTypes;
	/**
	 * The unique event types
	 */
	private String[] uniqueEventTypes;
	/**
	 * A query that inserts events into the database
	 */
	private static final String INSERT_EVENT_QUERY = "INSERT INTO EVENTS (EVENT_UUID, EVENT_DATASET_UUID, "
			+ " EVENT_TYPE_UUID, EVENT_START_TIME,"
			+ " EVENT_END_TIME, EVENT_PARENT_UUID, EVENT_POSITION, EVENT_CERTAINTY) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	/**
	 * A query that inserts event tags into the database
	 */
	private static final String TAG_INSERT_QUERY = "INSERT INTO TAGS (TAG_NAME, TAG_ENTITY_UUID, TAG_ENTITY_CLASS) VALUES (?,?,?)";

	/**
	 * Creates a Events object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public Events(Connection dbCon) throws Exception {
		this.dbCon = dbCon;
		this.eventDatasetUuid = null;
		this.eventTypes = null;
		this.eventPositions = null;
		this.eventStartTimes = null;
		this.eventEndTimes = null;
		this.eventCertainties = null;
		this.uniqueEventTypes = null;
		atb = new Attributes(dbCon);
		eventTypeTagMap = new HashMap<String, EventTypeTags>();
		eventstmt = dbCon.prepareStatement(INSERT_EVENT_QUERY);
		tagstmt = dbCon.prepareStatement(TAG_INSERT_QUERY);
	}

	/**
	 * Add the event attributes to a batch.
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
	public void addEventAttributes(String path, Double[] numericValues,
			String[] values) throws MobbedException {
		for (int i = 0; i < eventPositions.length; i++) {
			atb.reset(UUID.randomUUID(), eventUuids[i], "events",
					eventDatasetUuid, path, numericValues[i], values[i]);
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
	 * @return the UUIDs of the event eventTypes
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addEvents(boolean original) throws MobbedException {
		String[] eventTypeUuids = addEventTypes();
		if (original)
			initializeEventMap();
		eventUuids = new UUID[eventPositions.length];
		try {
			for (int i = 0; i < eventPositions.length; i++) {
				eventUuids[i] = UUID.randomUUID();
				eventstmt.setObject(1, eventUuids[i], Types.OTHER);
				eventstmt.setObject(2, eventDatasetUuid, Types.OTHER);
				eventstmt
						.setObject(3, eventTypeTagMap.get(eventTypes[i]
								.toUpperCase()).eventTypeUuid, Types.OTHER);
				eventstmt.setDouble(4, eventStartTimes[i]);
				eventstmt.setDouble(5, eventEndTimes[i]);
				eventstmt.setObject(6, eventMap.get(originalEventPositions[i]),
						Types.OTHER);
				eventstmt.setLong(7, eventPositions[i]);
				eventstmt.setDouble(8, eventCertainties[i]);
				eventstmt.addBatch();
				eventMap.put(eventPositions[i], eventUuids[i]);
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
	 * @param eventDatasetUuid
	 *            the UUID of the dataset associated with the events
	 * @param eventStartTimes
	 *            the start times of the events
	 * @param eventEndTimes
	 *            the end times of the events
	 * @param originalEventPositions
	 *            the original eventPositions of the events
	 * @param eventPositions
	 *            the eventPositions of the events
	 * @param eventCertainties
	 *            the eventCertainties of the events
	 * @param uniqueEventTypes
	 *            the unique event eventTypes
	 * @param eventTypes
	 *            all of the event eventTypes
	 * @param existingEventTypeUuids
	 *            the UUIDs of the event eventTypes that will be reused
	 * @param eventTags
	 *            the tags that are associated with the events
	 * @param eventTypeTags
	 *            the tags that are associated with the event eventTypes
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void reset(String eventDatasetUuid, double[] eventStartTimes,
			double[] eventEndTimes, long[] originalEventPositions,
			long[] eventPositions, double[] eventCertainties,
			String[] eventTypeDescriptions, String uniqueEventTypes[],
			String[] eventTypes, String[] existingEventTypeUuids,
			HashMap<Long, String[]> eventTags,
			HashMap<String, String[]> eventTypeTags) {
		this.eventDatasetUuid = UUID.fromString(eventDatasetUuid);
		this.eventStartTimes = eventStartTimes;
		this.eventEndTimes = eventEndTimes;
		this.originalEventPositions = originalEventPositions;
		this.eventPositions = eventPositions;
		this.eventCertainties = eventCertainties;
		this.eventTypeDescriptions = eventTypeDescriptions;
		this.uniqueEventTypes = uniqueEventTypes;
		this.eventTypes = eventTypes;
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
			for (int i = 0; i < eventPositions.length; i++) {
				if (!ManageDB.isEmpty(eventTags.get(eventPositions[i]))) {
					String[] tags = eventTags.get(eventPositions[i]);
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
	 * Adds a new event type if it does not already exist. Event eventTypes
	 * should be reused when storing datasets that have event eventTypes with
	 * the same meaning.
	 * 
	 * @return the UUIDs of the event eventTypes
	 * @throws MobbedException
	 *             if an error occurs
	 */
	private String[] addEventTypes() throws MobbedException {
		eventTypeTagMap = EventTypes.addEventTypes(dbCon,
				existingEventTypeUuids, eventTypeDescriptions,
				uniqueEventTypes, eventTypeTags);
		String[] eventTypeUuids = EventTypes.getStringValues(eventTypeTagMap);
		return eventTypeUuids;
	}

	/**
	 * Initializes the eventMap field that maps the event eventPositions to the
	 * event uuids.
	 */
	private void initializeEventMap() {
		eventMap = new HashMap<Long, UUID>();
		int numPos = originalEventPositions.length;
		for (int i = 0; i < numPos; i++) {
			eventMap.put(originalEventPositions[i],
					UUID.fromString(ManageDB.NO_PARENT_UUID));
		}
	}

}
