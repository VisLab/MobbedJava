package edu.utsa.mobbed;

import java.util.HashMap;
import java.util.UUID;

public class EventTypeTags {
	/**
	 * The UUID of the event type
	 */
	public UUID eventTypeUuid;
	/**
	 * A hashmap containing the tags associated with the event type
	 */
	public HashMap<String, String> tags;

	/**
	 * Creates a EventTypeTags object.
	 * 
	 * @param eventTypeUuid
	 *            the UUID of the event type
	 * @param tags
	 *            a hashmap containing the tags associated with the event type
	 */
	public EventTypeTags(UUID eventTypeUuid, HashMap<String, String> tags) {
		this.eventTypeUuid = eventTypeUuid;
		this.tags = tags;
	}

	/**
	 * gets the event type UUID
	 * 
	 * @return event type UUID
	 */
	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	/**
	 * get the tags
	 * 
	 * @return a hashmap containing the tags
	 */
	public HashMap<String, String> getTags() {
		return tags;
	}

}
