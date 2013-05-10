/**
 * 
 */
package edu.utsa.testmobbed;

import org.junit.BeforeClass;

import edu.utsa.mobbed.ManageDB;

/**
 * @author JCockfield
 * 
 */
public class TestStructures {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "structuredb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;

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

		}
	}

}
