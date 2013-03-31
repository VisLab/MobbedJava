package edu.utsa.mobbed;

/**
 * This class lists the constants used in this package. Various data types,
 * entity classes, sizes of data types are represented with distinct integer
 * values.
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */
public class MobbedConstants {

	/**
	 * Handler for required structure field
	 */
	public static final int HANDLER_REQUIRED = 2001;
	/**
	 * Handler for optional structure field
	 */
	public static final int HANDLER_OPTIONAL = 2002;
	/**
	 * Handler for manual structure field
	 */
	public static final int HANDLER_MANUAL = 2003;

	/**
	 * Size of an integer in bytes
	 */
	public static final int INT_BYTES = 4;
	/**
	 * Size of an double in bytes
	 */
	public static final int DOUBLE_BYTES = 8;
	/**
	 * Size of an long in bytes
	 */
	public static final int LONG_BYTES = 8;
	/**
	 * Size of an short in bytes
	 */
	public static final int SHORT_BYTES = 2;

	public static final String USER_DEFINED_FUNCTIONS = "CREATE OR REPLACE FUNCTION extractRange(inQuery varchar, outQuery varchar, lower double precision,"
			+ " upper double precision) RETURNS SETOF RECORD AS $$"
			+ " DECLARE"
			+ " inevent EVENTS;"
			+ " outevent EVENTS;"
			+ " founduuids uuid[];"
			+ " inuuid uuid;"
			+ " i integer := 1;"
			+ " BEGIN"
			+ " FOR inevent in EXECUTE inQuery"
			+ " LOOP"
			+ " FOR outevent in EXECUTE outQuery ||"
			+ " ' INTERSECT SELECT * FROM EVENTS"
			+ " WHERE EVENT_UUID <> $1 AND EVENT_ENTITY_UUID = $2 AND EVENT_START_TIME BETWEEN $3 + $5 AND $4 + $6'"
			+ " USING inevent.event_uuid, inevent.event_entity_uuid, inevent.event_start_time, inevent.event_end_time, lower, upper"
			+ " Loop"
			+ " founduuids[i] := outevent.event_uuid;"
			+ " i := i+1;"
			+ " END LOOP;"
			+ " IF outevent IS NOT NULL THEN"
			+ " RETURN QUERY SELECT *, founduuids AS FOUND_UUIDS FROM EVENTS WHERE EVENT_UUID = inevent.event_uuid;"
			+ " END IF;"
			+ " i := 1;"
			+ " founduuids := NULL;"
			+ " END LOOP;"
			+ " RETURN;" + "END;" + "$$ LANGUAGE plpgsql;";

}
