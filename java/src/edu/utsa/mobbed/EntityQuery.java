package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

public class EntityQuery {

	/**
	 * Database column types
	 */
	private static final String[] DB_TYPES = { "uuid", "character varying",
			"array", "integer", "bigint", "double precision",
			"timestamp without time zone", "oid" };
	/**
	 * JDBC column types
	 */
	private static final int[] JDBC_TYPES = { Types.OTHER, Types.VARCHAR,
			Types.ARRAY, Types.INTEGER, Types.BIGINT, Types.DOUBLE,
			Types.OTHER, Types.BIGINT };

	/**
	 * Constructs a query that's associated with a entity
	 * 
	 * @param md
	 *            ManageDB object
	 * @param regex
	 *            on if using regular expressions, off if otherwise
	 * @param cols
	 *            the names of the database columns
	 * @param vals
	 *            the values associated with the database columns
	 * @param dcols
	 *            the names of the double database columns
	 * @param dvals
	 *            the double values associated with the database columns
	 * @param ranges
	 *            the ranges associated with double columns
	 * @return a query that's associated with a entity
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public static String constructQuery(ManageDB md, String regex,
			String[] cols, String[][] vals, String[] dcols, Double[][] dvals,
			double[][] ranges) throws MobbedException {
		String qry = new String();
		boolean colEmpty = true;
		if (!ManageDB.isEmpty(cols) || !ManageDB.isEmpty(dcols))
			qry = ManageDB.concatStrs(qry, "WHERE");
		if (!ManageDB.isEmpty(cols)) {
			qry = ManageDB
					.concatStrs(qry, addConditions(md, regex, cols, vals));
			colEmpty = false;
		}
		if (!ManageDB.isEmpty(dcols))
			qry = ManageDB.concatStrs(qry,
					addDoubleConditions(md, colEmpty, dcols, dvals, ranges));
		return qry;
	}

	/**
	 * Add conditions to query
	 * 
	 * @param md
	 *            ManageDB object
	 * @param regex
	 *            on if using regular expressions, off if otherwise
	 * @param cols
	 *            the names of the database columns
	 * @param vals
	 *            the values associated with the database columns
	 * @return query conditions
	 */
	private static String addConditions(ManageDB md, String regex,
			String[] cols, String[][] vals) {
		String qry = new String();
		HashMap<String, Integer> jdbcMap = intitializeJDBCMap();
		for (int i = 0; i < cols.length; i++) {
			int type = jdbcMap.get(md.getColumnType(cols[i].toLowerCase()));
			qry = ManageDB.concatStrs(qry, "AND");
			if (isString(md, cols[i]))
				qry = addStringCondition(md, regex, qry, cols[i], vals[i], type);
			else
				qry = addOtherCondition(md, qry, cols[i], vals[i], type);
		}
		qry = qry.replaceFirst("AND ", "");
		return qry;
	}

	/**
	 * Add a double condition to a query
	 * 
	 * @param md
	 *            ManageDB object
	 * @param qry
	 *            the query to add the condition to
	 * @param dcol
	 *            the name of the double database column
	 * @param dvals
	 *            the values associated with the double database column
	 * @param ranges
	 *            the range values associated with the double database column
	 * @return a query with double condition added to
	 */
	private static String addDoubleCondition(ManageDB md, String qry,
			String dcol, Double[] dvals, double[] ranges) {
		String cond = new String();
		cond = assignConditionValues(md.getConnection(),
				buildDoubleCondition(dcol, dvals), dvals, ranges);
		return ManageDB.concatStrs(qry, cond);
	}

	/**
	 * Add double conditions to a query
	 * 
	 * @param md
	 *            ManageDB object
	 * @param colEmpty
	 *            true if the non-double columns are empty, false if otherwise
	 * @param dcols
	 *            the names of the double database columns
	 * @param dvals
	 *            the values associated with the double database columns
	 * @param ranges
	 *            the range values associated with the double database column
	 * @return the double conditions
	 */
	private static String addDoubleConditions(ManageDB md, boolean colEmpty,
			String[] dcols, Double[][] dvals, double[][] ranges) {
		String qry = new String();
		if (!colEmpty)
			qry = "AND";
		String subQry = "(";
		for (int i = 0; i < dcols.length; i++) {
			subQry = ManageDB.concatStrs(subQry, "AND");
			subQry = addDoubleCondition(md, subQry, dcols[i], dvals[i],
					ranges[i]);
		}
		subQry = subQry.replaceFirst("AND ", "");
		subQry += ")";
		return ManageDB.concatStrs(qry, subQry);
	}

	/**
	 * Adds a non-string query condition to a query
	 * 
	 * @param md
	 *            ManageDB object
	 * @param qry
	 *            the query to add the condition to
	 * @param col
	 *            the name of the database column
	 * @param vals
	 *            the values associated with the database column
	 * @param type
	 *            the JDBC type associated with the query condition
	 * @return a query with the added condition
	 */
	private static String addOtherCondition(ManageDB md, String qry,
			String col, String[] vals, int type) {
		String cond = new String();
		cond = assignConditionValues(md.getConnection(),
				buildOtherCondition(col, vals), vals, type);
		return ManageDB.concatStrs(qry, cond);
	}

	/**
	 * Adds a string query condition to a query
	 * 
	 * @param md
	 *            ManageDB object
	 * @param regex
	 *            on if using regular expressions, off if otherwise
	 * @param qry
	 *            the query to add the condition to
	 * @param col
	 *            the name of the database column
	 * @param vals
	 *            the values associated with the database column
	 * @param type
	 *            the JDBC type associated with the query condition
	 * @return a query with the added condition
	 */
	private static String addStringCondition(ManageDB md, String regex,
			String qry, String col, String[] vals, int type) {
		String cond = new String();
		if ("on".equals(regex.toLowerCase()))
			cond = assignConditionValues(md.getConnection(),
					buildRegexCondition(col, vals), vals, type);
		else
			cond = assignConditionValues(md.getConnection(),
					buildStringCondition(col, vals), vals, type);
		return ManageDB.concatStrs(qry, cond);
	}

	/**
	 * Assign the double query condition values
	 * 
	 * @param con
	 *            a connection to the database
	 * @param cond
	 *            the query condition
	 * @param dvals
	 *            the double values associated with the query condition
	 * @param range
	 *            the range values associated with the query condition
	 * @return a double query condition with assigned values
	 */
	private static String assignConditionValues(Connection con, String cond,
			Double[] dvals, double[] range) {
		try {
			PreparedStatement smt = con.prepareStatement(cond);
			int i = 1;
			for (Double dval : dvals) {
				smt.setDouble(i, dval + range[0]);
				smt.setDouble(i + 1, dval + range[1]);
				i += 2;
			}
			cond = smt.toString();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		return cond;
	}

	/**
	 * Assign the query condition values
	 * 
	 * @param con
	 *            a connection to the database
	 * @param cond
	 *            the query condition
	 * @param vals
	 *            the values associated with the query condition
	 * @param type
	 *            the JDBC type associated with the query condition
	 * @return a query condition with assigned values
	 */
	private static String assignConditionValues(Connection con, String cond,
			String[] vals, int type) {
		try {
			PreparedStatement smt = con.prepareStatement(cond);
			for (int i = 0; i < vals.length; i++) {
				smt.setObject(i + 1, vals[i], type);
			}
			cond = smt.toString();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		return cond;
	}

	/**
	 * Builds a double query condition
	 * 
	 * @param dcol
	 *            the name of the double database column
	 * @param dvals
	 *            the values associated with the double database column
	 * @return a double query condition
	 */
	private static String buildDoubleCondition(String dcol, Double[] dvals) {
		String cond = new String();
		for (int i = 0; i < dvals.length; i++) {
			cond = ManageDB.concatStrs(cond, dcol + " BETWEEN ? AND ?");
			cond = ManageDB.concatStrs(cond, "OR");
		}
		return cond.replaceFirst("OR$", "");
	}

	/**
	 * Builds a non-string query condition
	 * 
	 * @param col
	 *            the name of the database column
	 * @param vals
	 *            the values associated with the database column
	 * @return a non-string query condition
	 */
	private static String buildOtherCondition(String col, String[] vals) {
		String cond = new String();
		for (int i = 0; i < vals.length; i++) {
			if (i == 0)
				cond = ManageDB.concatStrs(col, "IN(");
			cond += "?,";
			if (i == vals.length - 1)
				cond = cond.replaceFirst(",$", ")");
		}
		return cond;
	}

	/**
	 * Builds a regular expression query condition
	 * 
	 * @param col
	 *            the name of the database column
	 * @param vals
	 *            the values associated with the database column
	 * @return a regular expression query condition
	 */
	private static String buildRegexCondition(String col, String[] vals) {
		String cond = new String();
		for (int i = 0; i < vals.length; i++) {
			cond = ManageDB.concatStrs(col, "~* ?");
			cond = ManageDB.concatStrs(cond, "OR");
		}
		return cond.replaceFirst("OR$", "");
	}

	/**
	 * Builds a string query condition
	 * 
	 * @param col
	 *            the name of the database column
	 * @param vals
	 *            the values associated with the database column
	 * @return a string query condition
	 */
	private static String buildStringCondition(String col, String[] vals) {
		String cond = new String();
		for (int i = 0; i < vals.length; i++) {
			if (i == 0)
				cond = ManageDB.concatStrs("LOWER(" + col + ")", "IN(");
			cond += "LOWER(?),";
			if (i == vals.length - 1)
				cond = cond.replaceFirst(",$", ")");
		}
		return cond;
	}

	/**
	 * Initializes a HashMap that contains database types and JDBC types
	 * 
	 * @return a HashMap
	 */
	private static HashMap<String, Integer> intitializeJDBCMap() {
		HashMap<String, Integer> jdbcMap = new HashMap<String, Integer>();
		for (int i = 0; i < DB_TYPES.length; i++)
			jdbcMap.put(DB_TYPES[i], JDBC_TYPES[i]);
		return jdbcMap;
	}

	/**
	 * Checks to see if the database column is character varying or a string
	 * 
	 * @param md
	 *            ManageDB object
	 * @param col
	 *            the name of the database column
	 * @return true if a string, false if otherwise
	 */
	private static boolean isString(ManageDB md, String col) {
		return "character varying".equals(md.getColumnType(col.toLowerCase()));
	}
}
