/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.EventTypes;
import edu.utsa.mobbed.ManageDB;

/**
 * Unit tests for EventTypes class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestEventTypes {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "eventtypedb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;
	private static EventTypes etype;
	private static String[] eventTypeUuids;

	@BeforeClass
	public static void setup() throws Exception {
		try {
			md = new ManageDB(name, hostname, user, password, verbose);
		} catch (Exception e) {
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
			eventTypeUuids = new String[2];
			etype = new EventTypes(md.getConnection());
			etype.reset("etype1", "etype1 description");
			etype.save();
			eventTypeUuids[0] = etype.getEventTypeUuid().toString();
			etype.reset("etype2", "etype2 description");
			etype.save();
			eventTypeUuids[1] = etype.getEventTypeUuid().toString();
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

	@Test
	public void testRetrieveMap() throws Exception {
		System.out.println("Unit test for retrieveMap");
		System.out
				.println("It should retrieve event types from the database and put them in a hash map");
		etype.retrieveMap(eventTypeUuids);
		System.out.println("--The event type map should contain etype1");
		assertTrue("The event type map does not contain etype1",
				etype.containsEventType("etype1".toUpperCase()));
		System.out
				.println("--The event type map should return the uuid of etype1");
		assertNotNull("The event type map does not return the uuid of etype1",
				etype.getEventTypeUuid("etype1".toUpperCase()));
	}

}
