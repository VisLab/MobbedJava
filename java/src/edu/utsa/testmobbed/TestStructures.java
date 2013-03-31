/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.assertTrue;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.MobbedConstants;
import edu.utsa.testmobbed.helpers.Structures;


/**
 * @author JCockfield
 * 
 */
public class TestStructures {
	private String tablePath = Class.class
			.getResource("/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose;
	private ManageDB md;
	private Structures structure1;

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
		} finally {
			structure1 = new Structures(md.getConnection());
			structure1.reset(UUID.randomUUID(), "parentStructure",
					MobbedConstants.HANDLER_REQUIRED, null);
		}
	}

	@Test
	public void testAddRetrieve() throws Exception {
		System.out
				.println("TEST: testing retrieve. There should be a new structure created because structure doesn't exist");
		Structures newStruct = Structures.retrieve(md.getConnection(),
				"newStructure", null, false);
		assertTrue("New structure created is not null", newStruct != null);
		assertTrue("New structure created is named newStructure", newStruct
				.getStructureName().equals("newStructure"));

		structure1.save();
		System.out
				.println("TEST: testing retrieve. There should be an existing strucuture retrieved");
		Structures existingStruct = Structures.retrieve(md.getConnection(),
				"parentStructure", null, false);

		System.out
				.println("TEST: testing retrieve. There should be no children associated with an existing strucuture retrieved");
		existingStruct = Structures.retrieve(md.getConnection(),
				"parentStructure", null, true);

		System.out
				.println("TEST: testing retrieve. There should be one child associated with an existing strucuture retrieved");
		UUID struct1Uuid = structure1.getStructureUuid();
		structure1.reset(UUID.randomUUID(), "childStructure1",
				MobbedConstants.HANDLER_REQUIRED, struct1Uuid);
		structure1.save();
		existingStruct = Structures.retrieve(md.getConnection(),
				"parentStructure", null, true);
		assertTrue("Size of children array is not 1",
				existingStruct.getChildNames().length == 1);
		assertTrue("Existing structure contains child name",
				existingStruct.containsChild(structure1.getStructureName()));

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

}
