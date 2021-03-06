/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.EventTypeTags;
import edu.utsa.mobbed.EventTypes;
import edu.utsa.mobbed.ManageDB;

/**
 * Unit tests for EventTypes class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestEventTypes {
	private static EventTypes etype;
	private static String[] eventTypeUuids;
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "eventtypedb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testRetrieveMap() throws Exception {
		System.out.println("Unit test for retrieveMap");
		System.out
				.println("It should retrieve event types from the database and put them in a hash map");
		String[] uniqueTypes = {};
		String[][] eventTypeTags = { {} };
		HashMap<String, EventTypeTags> eventTypeTagMap = EventTypes
				.addNewEventTypes(md.getConnection(), eventTypeUuids,
						uniqueTypes, eventTypeTags);
		System.out.println("--The event type map should contain etype1");
		assertTrue("The event type map does not contain etype1",
				eventTypeTagMap.containsKey("etype1".toUpperCase()));
		System.out
				.println("--The event type map should return the uuid of etype1");
		assertNotNull("The event type map does not return the uuid of etype1",
				eventTypeTagMap.get("etype1".toUpperCase()).eventTypeUuid);
	}

	@BeforeClass
	public static void setup() throws Exception {
		try {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
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

}
