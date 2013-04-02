/**
 * 
 */
package edu.utsa.testmobbed;

import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.Metadata;
import edu.utsa.testmobbed.helpers.Datasets;

/**
 * @author JCockfield
 * 
 */
public class TestMetadata {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose;
	private ManageDB md;
	private Datasets dataset1;
	private Metadata metadata1;
	private UUID datadefUuid;

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
			dataset1 = new Datasets(md.getConnection());
			dataset1.reset(false, "ELEMENT_DATASET",
					"691df7dd-ce3e-47f8-bea5-6a632c6fcccb",
					"DATASET FOR ELEMENTS", "ELEMENT_NAMESPACE",
					"791df7dd-ce3e-47f8-bea5-6a632c6fcccb", null);
			dataset1.save();
			datadefUuid = UUID.randomUUID();
			metadata1 = new Metadata(md.getConnection());
			metadata1.reset(dataset1.getDatasetUuid().toString(),
					datadefUuid.toString());
		}

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testAddAttributes() throws Exception {
		String fieldName = "MetadataField1";
		Double[] numAttrValues1 = { 0.123 };
		String[] attrValues1 = { "0.123" };
		metadata1.addAttribute(fieldName, numAttrValues1, attrValues1);
		fieldName = "MetadataField2";
		Double[] numAttrValues2 = { 0.456 };
		String[] attrValues2 = { "0.456" };
		metadata1.addAttribute(fieldName, numAttrValues2, attrValues2);
		metadata1.save();
	}

	@Test
	public void testAddFields() throws Exception {
		String fieldName1 = "MetadataField1";
		Double[] numAttrValues1 = { 0.123 };
		String[] attrValues1 = { "0.123" };
		metadata1.addAttribute(fieldName1, numAttrValues1, attrValues1);
		String fieldName2 = "MetadataField2";
		Double[] numAttrValues2 = { 0.123 };
		String[] attrValues2 = { "0.123" };
		metadata1.addAttribute(fieldName2, numAttrValues2, attrValues2);
		metadata1.save();
	}
}