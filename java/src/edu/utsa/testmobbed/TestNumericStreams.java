/**
 * 
 */
package edu.utsa.testmobbed;

import edu.utsa.mobbed.ManageDB;

/**
 * Unit tests for NumericStreams class
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class TestNumericStreams {
	private static String tablePath = Class.class.getResource(
			"/edu/utsa/testmobbed/mobbed.sql").getPath();
	private static String name = "attributedb";
	private static String hostname = "localhost";
	private static String user = "postgres";
	private static String password = "admin";
	private static boolean verbose = false;
	private static ManageDB md;

}
