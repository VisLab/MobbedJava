/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.EventTypes;
import edu.utsa.mobbed.Events;
import edu.utsa.mobbed.ManageDB;
import edu.utsa.testmobbed.helpers.Datasets;

/**
 * @author JCockfield
 * 
 */
public class TestEventTypes {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose = true;
	private ManageDB md;
	private EventTypes eventtype1;

	@Before
	public void setUp() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			eventtype1 = new EventTypes(md.getConnection());
		}

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testRetrieveName2UuidMap() throws Exception {
		eventtype1.reset("event type 1", "event type description 1");
		eventtype1.save();
		String[] eventTypeUuids = { eventtype1.getEventTypeUuid().toString() };
		eventtype1.retrieveName2UuidMap(eventTypeUuids);
		assertTrue("name2uuid map size is not 1", eventtype1.getname2UuidMap()
				.size() == 1);
		assertTrue("name2uuid map size is not 1", eventtype1.getname2UuidMap()
				.containsKey(eventtype1.getEventType().toUpperCase()));
	}

	@Test
	public void testRetrieveUuid2NameMap() throws Exception {
		assertTrue("uuid2Name map size is not ", eventtype1.getuuid2NameMap()
				.size() == 0);
		Datasets dataset1 = new Datasets(md.getConnection());
		dataset1.reset(false, "EVENT_TYPE_DATASET",
				"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
				"Test case for event types", "EVENT_TYPE_NAMESPACE",
				"791df7dd-ce3e-47f8-bea5-6a632c6fcccb", null);
		dataset1.save();
		long[] positions = new long[] { 1 };
		String[] eventTypes = { "evType1" };
		double[] eventLatencies = { 111 };
		double[] eventCertainties = { 1.0 };
		String[] defaultFields = {};
		Events event1 = new Events(md.getConnection());
		event1.reset(dataset1.getDatasetUuid().toString(), "event",
				defaultFields, eventTypes, eventTypes, positions,
				eventLatencies, eventLatencies, eventCertainties, null, null);
		event1.addEvents();
		event1.save();
		eventtype1.retrieveUuid2NameMap(dataset1.getDatasetUuid());
		assertTrue("uuid2Name map size is not 1", eventtype1.getuuid2NameMap()
				.size() == 1);
	}

}
