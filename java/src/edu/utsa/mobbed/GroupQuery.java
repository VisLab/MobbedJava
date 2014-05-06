package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class GroupQuery {

	/**
	 * Group delimiter
	 */
	private static final String GROUP_DELIMITER = "|";
	/**
	 * Group matches
	 */
	private static final String[] GROUP_MATCHES = { "EXACT", "PREFIX", "WORD",
			"ON", "OFF" };
	/**
	 * Group queries
	 */
	private static final String[] GROUP_QUERIES = {
			"INNER JOIN (SUBQUERY) AS TAG_ENTITIES ON TABLE.TABLE_KEY = TAG_ENTITIES.TAG_ENTITY_UUID",
			"INNER JOIN (SUBQUERY) AS ATTRIBUTES ON TABLE.TABLE_KEY = ATTRIBUTES.ATTRIBUTE_ENTITY_UUID" };
	/**
	 * Group regular expressions
	 */
	private static final String[] GROUP_REGEXS = { "^GROUP$", "^GROUP/*",
			"(^|/)GROUP(/|$)", "GROUP", "^GROUP$" };
	/**
	 * Group sub queries
	 */
	private static final String[] GROUP_SUB_QUERIES = {
			"INTERSECT SELECT DISTINCT TAG_ENTITY_UUID FROM TAG_ENTITIES INNER JOIN TAGS ON TAG_ENTITIES.TAG_ENTITY_TAG_UUID = TAGS.TAG_UUID WHERE TAGS.TAG_NAME ~* ? AND LOWER(TAG_ENTITY_CLASS) = LOWER(?)",
			"INTERSECT SELECT DISTINCT ATTRIBUTE_ENTITY_UUID FROM ATTRIBUTES WHERE ATTRIBUTE_VALUE ~* ? AND LOWER(ATTRIBUTE_ENTITY_CLASS) = LOWER(?)" };
	/**
	 * Group types
	 */
	private static final String[] GROUP_TYPES = { "TAGS", "ATTRIBUTES" };

	/**
	 * Constructs a query that's associated with a group
	 * 
	 * @param con
	 *            a connection to the database
	 * @param table
	 *            the table that's associated with group
	 * @param key
	 *            the key column associated with the table
	 * @param type
	 *            the type of group "TAGS" or "ATTRIBUTES"
	 * @param match
	 *            the group match "on","off","exact","prefix","word"
	 * @param vals
	 *            the group values
	 * @return a query that's associated with a group
	 */
	public static String constructGroupQuery(Connection con, String table,
			String key, String type, String match, String[][] vals) {
		String qry = new String();
		if (!ManageDB.isEmpty(vals)) {
			qry = generateGroupQuery(type, vals, table, key);
			String[] delimitedGroupValues = generateGroupRegexs(match, vals);
			PreparedStatement smt = assignGroupQueryValues(con, table, qry,
					delimitedGroupValues);
			qry = smt.toString();
		}
		return qry;
	}

	/**
	 * Assign values to a query
	 * 
	 * @param con
	 *            a connection to the database
	 * @param table
	 *            the table that's associated with a group
	 * @param qry
	 *            the query to assign values to
	 * @param regexs
	 *            the regular expressions with assigned values
	 * @return a prepared statement containing the query with assigned values
	 */
	private static PreparedStatement assignGroupQueryValues(Connection con,
			String table, String qry, String[] regexs) {
		PreparedStatement smt = null;
		try {
			smt = con.prepareStatement(qry, ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			int j = 1;
			for (int i = 0; i < regexs.length; i++) {
				smt.setString(j++, regexs[i]);
				smt.setString(j++, table);
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		return smt;
	}

	/**
	 * Delimits the group values
	 * 
	 * @param vals
	 *            the values of the group
	 * @return the delimited group values
	 */
	private static String[] delimitGroupValues(String[][] vals) {
		String[] str = new String[vals.length];
		for (int i = 0; i < vals.length; i++) {
			str[i] = "(";
			for (int j = 0; j < vals[i].length; j++) {
				str[i] += vals[i][j].replaceAll("/$", "") + GROUP_DELIMITER;
			}
			str[i] = str[i].replaceAll("\\" + GROUP_DELIMITER + "$", "");
			str[i] += ")";
		}
		return str;
	}

	/**
	 * Generates a group query
	 * 
	 * @param groupType
	 *            the type of group "TAGS" or "ATTRIBUTES"
	 * @param groupValues
	 *            the values of the group
	 * @param table
	 *            the table that's associated with the group
	 * @param key
	 *            the key that's associated with the table
	 * @return a group query
	 */
	private static String generateGroupQuery(String groupType,
			String[][] groupValues, String table, String key) {
		HashMap<String, String> groupQryMap = initializeHashMap(GROUP_TYPES,
				GROUP_QUERIES);
		HashMap<String, String> groupSubQryMap = initializeHashMap(GROUP_TYPES,
				GROUP_SUB_QUERIES);
		String qry = groupQryMap.get(groupType.toUpperCase());
		String subqry = new String();
		qry = qry.replaceFirst("TABLE", table.toUpperCase());
		qry = qry.replaceFirst("TABLE_KEY", key.toUpperCase());
		for (int i = 0; i < groupValues.length; i++) {
			subqry = ManageDB.concatStrs(subqry,
					groupSubQryMap.get(groupType.toUpperCase()));
		}
		subqry = subqry.replaceFirst("INTERSECT ", "");
		qry = qry.replaceFirst("SUBQUERY", subqry);
		return qry;
	}

	/**
	 * Generates regular expressions
	 * 
	 * @param match
	 *            the group match "on", "off", "exact", "prefix", "word"
	 * @param vals
	 *            the group values
	 * @return group regular expressions
	 */
	private static String[] generateGroupRegexs(String match, String[][] vals) {
		String[] delimitedVals = delimitGroupValues(vals);
		HashMap<String, String> regexMap = initializeHashMap(GROUP_MATCHES,
				GROUP_REGEXS);
		String[] regexs = new String[delimitedVals.length];
		String regex = regexMap.get(match.toUpperCase());
		for (int i = 0; i < delimitedVals.length; i++) {
			regexs[i] = regex.replaceAll("GROUP", delimitedVals[i]);
		}
		return regexs;
	}

	/**
	 * Initializes a HashMap
	 * 
	 * @param keys
	 *            the keys of the HashMap
	 * @param vals
	 *            the values of the HashMap
	 * @return a HashMap
	 */
	private static HashMap<String, String> initializeHashMap(String[] keys,
			String[] vals) {
		HashMap<String, String> hMap = new HashMap<String, String>();
		for (int i = 0; i < keys.length; i++) {
			hMap.put(keys[i], vals[i]);
		}
		return hMap;
	}
}
