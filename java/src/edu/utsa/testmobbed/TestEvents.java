package edu.utsa.testmobbed;

import static org.junit.Assert.assertEquals;

import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.*;

/**
 * Unit tests for Events class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestEvents {
	private double[] derievedEventCertainties = { 1.0, 1.0 };
	private double[] derivedEventLatencies = { 222, 444 };
	private String[] derivedEventTypes = { "et1", "et3" };
	private long[] derivedOriginalPositions = { 2, 4 };
	private long[] derivedPositions = { 1, 2 };
	private String[][] eventTypeTags = null;
	private String[] existingEventTypeUuids = null;
	private double[] originalEventCertainties = { 1.0, 1.0, 1.0, 1.0 };
	private double[] originalEventLatencies = { 111, 222, 333, 444 };
	private String[] originalEventTypes = { "et1", "et1", "et2", "et3" };
	private long[] originalPositions = { 1, 2, 3, 4 };
	private String[] originalUniqueEventTypes = { "et1", "et2", "et3" };
	private static String[] datasetUuids;
	private static Events event;
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "eventdb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@After
	public void cleanup() throws Exception {
		Statement stmt = md.getConnection().createStatement();
		String query = "DELETE FROM EVENTS";
		stmt.execute(query);
	}

	@Before
	public void setupTest() throws Exception {
		event = new Events(md.getConnection());
		event.reset(datasetUuids[0], originalEventLatencies,
				originalEventLatencies, originalPositions, originalPositions,
				originalEventCertainties, originalUniqueEventTypes,
				originalEventTypes, null, eventTypeTags);
		existingEventTypeUuids = event.addNewTypes();
		event.addEvents(true);
	}

	@Test
	public void testAddAttribute() throws Exception {
		System.out.println("Unit test for addAttribute");
		System.out.println("It should store 2 event attributes");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM ATTRIBUTES";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("--There should be no attributes in the database.");
		assertEquals("There are attributes in the database", expected, actual);
		String fieldName = "urevent";
		Double[] numAttrValues = { 1.0, 2.0 };
		String[] attrValues = { "1.0", "2.0" };
		event.save();
		event.reset(datasetUuids[0], derivedEventLatencies,
				derivedEventLatencies, derivedOriginalPositions,
				derivedPositions, derievedEventCertainties, derivedEventTypes,
				derivedEventTypes, existingEventTypeUuids, eventTypeTags);
		event.addNewTypes();
		event.addEvents(false);
		event.addAttribute(fieldName, numAttrValues, attrValues);
		event.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 2;
		actual = rs.getInt(1);
		System.out.println("--There should be 2 attributes in the database.");
		assertEquals("There are no attributes in the database", expected,
				actual);

	}

	@Test
	public void testAddEventsDerieved() throws Exception {
		System.out.println("Unit test for addEvents with derived events ");
		System.out
				.println("It should store 6 events. 4 original events, 2 derived events");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM EVENTS";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("--There should be no events in the database.");
		assertEquals("There are events in the database", expected, actual);
		event.save();
		event.reset(datasetUuids[0], derivedEventLatencies,
				derivedEventLatencies, derivedOriginalPositions,
				derivedPositions, derievedEventCertainties, derivedEventTypes,
				derivedEventTypes, existingEventTypeUuids, eventTypeTags);
		event.addNewTypes();
		event.addEvents(false);
		event.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 6;
		actual = rs.getInt(1);
		System.out.println("--There should be 6 events in the database.");
		assertEquals("There are no events in the database", expected, actual);
	}

	@Test
	public void testAddEventsOriginal() throws Exception {
		System.out.println("Unit test for addEvents with original events ");
		System.out.println("It should store 4 original events");
		int expected;
		int actual;
		Statement stmt = md.getConnection().createStatement();
		String query = "SELECT COUNT(*) FROM EVENTS";
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		expected = 0;
		actual = rs.getInt(1);
		System.out.println("--There should be no events in the database.");
		assertEquals("There are events in the database", expected, actual);
		event.save();
		rs = stmt.executeQuery(query);
		rs.next();
		expected = 4;
		actual = rs.getInt(1);
		System.out
				.println("--There should be 4 original events in the database.");
		assertEquals("There are no events in the database", expected, actual);
	}

	@BeforeClass
	public static void setup() throws Exception {
		try {
			ManageDB.deleteDatabase(name, hostname, user, password, verbose);
		} catch (Exception e) {
			tablePath = URLDecoder.decode(
					Class.class.getResource("/edu/utsa/testmobbed/mobbed.sql")
							.getPath(), "UTF-8");
			ManageDB.createDatabase(name, hostname, user, password, tablePath,
					verbose);
			md = new ManageDB(name, hostname, user, password, verbose);
		} finally {
			md.setAutoCommit(true);
			String datasetValues[][] = { { null, null, null, "EVENT_DATASET",
					null, null, null, "EVENT DATASET", null, null, null } };
			datasetUuids = md.addRows("datasets",
					md.getColumnNames("datasets"), datasetValues, null, null);
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}

}
