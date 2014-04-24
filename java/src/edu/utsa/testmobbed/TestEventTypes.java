/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;

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
	private static EventTypes etype;
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "eventtypedb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testConstructor() throws Exception {
		etype = new EventTypes(md.getConnection());
		etype.reset("eventType1", "event type 1 description");
		etype.save();
		assertNotNull(etype.getEventTypeUuid().toString());
		etype.reset("eventType2", "event type 2 description");
		etype.save();
		assertNotNull(etype.getEventTypeUuid().toString());
	}

	@BeforeClass
	public static void setup() throws Exception {
		try {
			ManageDB.dropDatabase(name, hostname, user, password, verbose);
		} catch (Exception e) {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.closeConnection();
		ManageDB.dropDatabase(name, hostname, user, password, verbose);
	}

}
