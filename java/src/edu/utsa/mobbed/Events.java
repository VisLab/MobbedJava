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
	private long[] originalPositions;
	/**
	 * The eventPositions of the events
	 */
	private long[] positions;
	/**
	 * The start times of the events
	 */
	private double[] startTimes;
	/**
	 * A prepared statement object that inserts tags into the database
	 */
	private PreparedStatement tagEntitystmt;
	/**
	 * A HashMap that contains tag uuids and tags
	 */
	private HashMap<String, UUID> tagMap;
	/**
	 * A prepared statement object that inserts tags into the database
	 */
	private PreparedStatement tagstmt;
	/**
	 * The descriptions of the event types
	 */
	private String[] typeDescriptions;
	/**
	 * The event types
	 */
	private String[] types;
	/**
	 * The unique event types
	 */
	private String[] uniqueEventTypes;
	/**
	 * A query that inserts events into the database
	 */
	private static final String EVENT_QRY = "INSERT INTO EVENTS (EVENT_UUID, EVENT_DATASET_UUID, "
			+ " EVENT_TYPE_UUID, EVENT_START_TIME,"
			+ " EVENT_END_TIME, EVENT_PARENT_UUID, EVENT_POSITION, EVENT_CERTAINTY) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	/**
	 * A query that inserts tag entities into the database
	 */
	private static final String TAG_ENTITY_QRY = "INSERT INTO TAG_ENTITIES (TAG_ENTITY_UUID, TAG_ENTITY_TAG_UUID,TAG_ENTITY_CLASS) VALUES (?,?,?)";
	/**
	 * A query that inserts tags into the database
	 */
	private static final String TAG_QRY = "INSERT INTO TAGS (TAG_UUID, TAG_NAME) VALUES (?,?)";

	/**
	 * Creates a Events object.
	 * 
	 * @param con
	 *            a connection to the database
	 */
	public Events(Connection con) throws Exception {
		this.dbCon = con;
		this.datasetUuid = null;
		this.types = null;
		this.positions = null;
		this.startTimes = null;
		this.endTimes = null;
		this.certainties = null;
		this.uniqueEventTypes = null;
		atb = new Attributes(con);
		eventTypeTagMap = new HashMap<String, EventTypeTags>();
		tagMap = new HashMap<String, UUID>();
		eventstmt = con.prepareStatement(EVENT_QRY);
		tagstmt = con.prepareStatement(TAG_QRY);
		tagEntitystmt = con.prepareStatement(TAG_ENTITY_QRY);
		InitializeTagMap();
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
	public void addAttribute(String path, Double[] numericValues,
			String[] values) throws MobbedException {
		for (int i = 0; i < positions.length; i++) {
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
	 * @return the UUIDs of the event eventTypes
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public String[] addEvents(boolean original) throws MobbedException {
		String[] eventTypeUuids = addEventTypes();
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
		this.datasetUuid = UUID.fromString(eventDatasetUuid);
		this.startTimes = eventStartTimes;
		this.endTimes = eventEndTimes;
		this.originalPositions = originalEventPositions;
		this.positions = eventPositions;
		this.certainties = eventCertainties;
		this.typeDescriptions = eventTypeDescriptions;
		this.uniqueEventTypes = uniqueEventTypes;
		this.types = eventTypes;
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
			tagEntitystmt.executeBatch();
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
						if (!tagMap.containsKey(tags[j].toUpperCase())) {
							UUID tagUuid = UUID.randomUUID();
							tagstmt.setObject(1, tagUuid, Types.OTHER);
							tagstmt.setString(2, tags[j]);
							tagstmt.addBatch();
							tagMap.put(tags[j].toUpperCase(), tagUuid);
						}
						tagEntitystmt.setObject(1, eventUuids[i], Types.OTHER);
						tagEntitystmt.setObject(2,
								tagMap.get(tags[j].toUpperCase()), Types.OTHER);
						tagEntitystmt.setString(3, "events");
						tagEntitystmt.addBatch();
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
				existingEventTypeUuids, typeDescriptions, uniqueEventTypes,
				eventTypeTags, tagMap);
		String[] eventTypeUuids = EventTypes.getStringValues(eventTypeTagMap);
		return eventTypeUuids;
	}

	/**
	 * Initializes the eventMap field that maps the event eventPositions to the
	 * event uuids.
	 */
	private void initializeEventMap() {
		eventMap = new HashMap<Long, UUID>();
		int numPos = originalPositions.length;
		for (int i = 0; i < numPos; i++) {
			eventMap.put(originalPositions[i],
					UUID.fromString(ManageDB.NO_PARENT_UUID));
		}
	}

	/**
	 * Initializes the tagMap field that maps tag names to tag uuids.
	 * 
	 * @throws MobbedException
	 */
	private void InitializeTagMap() throws MobbedException {
		String qry = "SELECT TAG_NAME, TAG_UUID FROM TAGS";
		try {
			Statement smt = dbCon.createStatement();
			ResultSet rs = smt.executeQuery(qry);
			while (rs.next()) {
				tagMap.put(rs.getString(1).toUpperCase(),
						UUID.fromString(rs.getString(2)));
			}
		} catch (SQLException ex) {
			throw new MobbedException("Could not retrieve tags\n"
					+ ex.getMessage());
		}

	}

}
