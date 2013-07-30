package edu.utsa.mobbed;

import java.util.HashMap;
import java.util.UUID;

public class EventTypeModel {
	public UUID eventTypeUuid;
	public HashMap<String, String> tagMap;

	public EventTypeModel(UUID eventTypeUuid, HashMap<String, String> tagMap) {
		this.eventTypeUuid = eventTypeUuid;
		this.tagMap = tagMap;
	}

	public UUID getEventTypeUuid() {
		return eventTypeUuid;
	}

	public HashMap<String, String> getTagMap() {
		return tagMap;
	}

}
