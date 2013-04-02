/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertTrue;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.EventTypeMaps;
import edu.utsa.mobbed.ManageDB;

/**
 * @author JCockfield
 * 
 */
public class TestEventTypeMaps {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose;
	private ManageDB md;

	@Before
	public void setUp() throws Exception {
		System.out
				.println("@Before - setUp - getting connection and generating database if it doesn't exist");
		try {
			md = new ManageDB(name, hostname, user, password);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password);
		}
	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testReset() throws Exception {
		EventTypeMaps evtmp = new EventTypeMaps(md.getConnection());
		System.out
				.println("TEST: testing reset. Event type map fields should be null");
		assertTrue("event type", evtmp.getEventTypeUuid() == null);
		assertTrue("event type entity uuid is not null",
				evtmp.getEventTypeEntityUuid() == null);
		UUID eventTypeUuid = UUID.randomUUID();
		UUID eventTypeEntityUuid = UUID.randomUUID();
		evtmp.reset(eventTypeUuid, eventTypeEntityUuid, "dataset");
		assertTrue("event type uuid is not equal", evtmp.getEventTypeUuid()
				.equals(eventTypeUuid));
		assertTrue("event type entity uuid is not equal", evtmp
				.getEventTypeEntityUuid().equals(eventTypeEntityUuid));
	}

	@Test
	public void testSave() throws Exception {
		EventTypeMaps evtmp = new EventTypeMaps(md.getConnection());
		System.out
				.println("TEST: testing save. There should be a event type map saved");
		evtmp.reset(UUID.randomUUID(), UUID.randomUUID(), "dataset");
		assertTrue("save should return 1", evtmp.save() == 1);
	}

}
