/**
 * 
 */
package edu.utsa.testmobbed;

import static org.junit.Assert.*;
import java.util.Random;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.utsa.mobbed.ManageDB;
import edu.utsa.mobbed.NumericStreams;

/**
 * @author JCockfield
 * 
 */
public class TestNumericStreams {
	private String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private String name = "testdb";
	private String hostname = "localhost";
	private String user = "postgres";
	private String password = "admin";
	private boolean verbose = true;
	private ManageDB md;
	private NumericStreams numStream1;
	private UUID datadefUuid;

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
			datadefUuid = UUID.randomUUID();
			numStream1 = new NumericStreams(md.getConnection());
			numStream1.reset(datadefUuid.toString());
		}

	}

	@After
	public void closeConnection() throws Exception {
		md.close();
	}

	@Test
	public void testRetrieveNumericStream() throws Exception {
		Random generator = new Random();
		double[][] values = new double[32][1000];
		double[] times = new double[1000];
		for (int i = 0; i < 1000; i++) {
			times[i] = generator.nextDouble();
			for (int j = 0; j < 32; j++)
				values[j][i] = generator.nextDouble();
		}
		numStream1.save(values, times, 1);
		int maxPosition = (int) numStream1.getMaxPosition();
		double[][] retrievedStream = numStream1.retrieveByPosition(1,
				maxPosition, 32);
		double[][] flippedStream = new double[retrievedStream[0].length][retrievedStream.length];
		// dimensions are flipped when retreived
		for (int i = 0; i < retrievedStream[0].length; i++) {
			for (int j = 0; j < retrievedStream.length; j++)
				flippedStream[i][j] = retrievedStream[j][i];
		}
		assertTrue("Length of retrieved stream is equal to stored stream",
				flippedStream.length == values.length);
		// assertTrue("Need to figure out how to do an equals with tolerance");

	}
}
