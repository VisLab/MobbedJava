package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class GroupQuery {

	private static final String GROUP_DELIMITER = "|";
	private static final String[] GROUP_MATCHES = { "EXACT", "PREFIX", "WORD",
			"ON", "OFF" };
	private static final String[] GROUP_QUERIES = {
			"INNER JOIN (SUBQUERY) AS TAG_ENTITIES ON TABLE.TABLE_KEY = TAG_ENTITIES.TAG_ENTITY_UUID",
			"INNER JOIN (SUBQUERY) AS ATTRIBUTES ON TABLE.TABLE_KEY = ATTRIBUTES.ATTRIBUTE_ENTITY_UUID" };
	private static final String[] GROUP_REGEXS = { "^GROUP$", "^GROUP/*",
			"(^|/)GROUP(/|$)", "GROUP", "^GROUP$" };
	private static final String[] GROUP_SUB_QUERIES = {
			"INTERSECT SELECT DISTINCT TAG_ENTITY_UUID FROM TAG_ENTITIES INNER JOIN TAGS ON TAG_ENTITIES.TAG_ENTITY_TAG_UUID = TAGS.TAG_UUID WHERE TAGS.TAG_NAME ~* ? AND LOWER(TAG_ENTITY_CLASS) = LOWER(?)",
			"INTERSECT SELECT DISTINCT ATTRIBUTE_ENTITY_UUID FROM ATTRIBUTES WHERE ATTRIBUTE_VALUE ~* ? AND LOWER(ATTRIBUTE_ENTITY_CLASS) = LOWER(?)" };
	private static final String[] GROUP_TYPES = { "TAGS", "ATTRIBUTES" };

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

	private static HashMap<String, String> initializeHashMap(String[] keys,
			String[] vals) {
		HashMap<String, String> hMap = new HashMap<String, String>();
		for (int i = 0; i < keys.length; i++) {
			hMap.put(keys[i], vals[i]);
		}
		return hMap;
	}
}
