/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;

import java.net.URLDecoder;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.Structures;

/**
 * Unit tests for Structures class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestStructures {
	private static String hostname = "localhost";
	private static ManageDB md;
	private static String name = "structuredb";
	private static String password = "admin";
	private static String tablePath;
	private static String user = "postgres";
	private static boolean verbose = false;

	@Test
	public void testRetrieve() throws Exception {
		System.out.println("Unit test for retrieve");
		System.out
				.println("It should retrieve the EEG structure along with all of its children");
		Structures EEGStruct = Structures.retrieve(md.getConnection(), "EEG",
				UUID.fromString(ManageDB.nullParentUuid), true);
		System.out.println("--EEG structure should have a child named element");
		assertTrue("element is not a child of EEG",
				EEGStruct.containsChild("element"));
		System.out
				.println("--EEG structure should return the uuid of the child named element");
		assertNotNull("element uuid is null",
				EEGStruct.getChildStructUuid("element"));
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
			Structures structure = new Structures(md.getConnection());
			UUID parentStructUuid = UUID.randomUUID();
			structure.reset(parentStructUuid, "EEG",
					UUID.fromString(ManageDB.nullParentUuid));
			structure.save();
			UUID elementStructUuid = UUID.randomUUID();
			structure.reset(elementStructUuid, "element", parentStructUuid);
			structure.save();
			UUID structureUuid = UUID.randomUUID();
			structure.reset(structureUuid, "X", elementStructUuid);
			structure.save();

		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		md.close();
		ManageDB.deleteDatabase(name, hostname, user, password, verbose);
	}
}
