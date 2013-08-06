package edu.utsa.mobbed;

import java.util.HashMap;
import java.util.UUID;

public class EventTypeTags {
	public UUID eventTypeUuid;
	public HashMap<String, String> tags;

	public EventTypeTags(UUID eventTypeUuid, HashMap<String, String> tags) {
		this.eventTypeUuid = eventTypeUuid;
		this.tags = tags;
	}

	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	public HashMap<String, String> getTags() {
		return tags;
	}

}
