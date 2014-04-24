package edu.utsa.mobbed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

public class EntityQuery {

	private static final String[] DB_TYPES = { "uuid", "character varying",
			"array", "integer", "bigint", "double precision",
			"timestamp without time zone", "oid" };
	private static final int[] JDBC_TYPES = { Types.OTHER, Types.VARCHAR,
			Types.ARRAY, Types.INTEGER, Types.BIGINT, Types.DOUBLE,
			Types.OTHER, Types.BIGINT };

	public static String constructQuery(ManageDB md, String regex,
			String[] cols, String[][] vals, String[] dcols, double[][] dvals,
			double[][] ranges) throws MobbedException {
		String qry = new String();
		if (!ManageDB.isEmpty(cols) || !ManageDB.isEmpty(dcols))
			qry = ManageDB.concatStrs(qry, "WHERE");
		if (!ManageDB.isEmpty(cols))
			qry = ManageDB
					.concatStrs(qry, addConditions(md, regex, cols, vals));
		if (!ManageDB.isEmpty(dcols))
			qry = ManageDB.concatStrs(qry,
					addDoubleConditions(md, dcols, dvals, ranges));
		return qry;
	}

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

	private static String addDoubleCondition(ManageDB md, String qry,
			String dcol, double[] dvals, double[] ranges) {
		String cond = new String();
		cond = assignConditionValues(md.getConnection(),
				buildDoubleCondition(dcol, dvals), dvals, ranges);
		return ManageDB.concatStrs(qry, cond);
	}

	private static String addDoubleConditions(ManageDB md, String[] dcols,
			double[][] dvals, double[][] ranges) {
		String qry = new String();
		for (int i = 0; i < dcols.length; i++) {
			qry = ManageDB.concatStrs(qry, "AND");
			qry = addDoubleCondition(md, qry, dcols[i], dvals[i], ranges[i]);
		}
		qry = qry.replaceFirst("AND ", "");
		return qry;
	}

	private static String addOtherCondition(ManageDB md, String qry,
			String col, String[] vals, int type) {
		String cond = new String();
		cond = assignConditionValues(md.getConnection(),
				buildOtherCondition(col, vals), vals, type);
		return ManageDB.concatStrs(qry, cond);
	}

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

	private static String assignConditionValues(Connection con, String cond,
			double[] dvals, double[] range) {
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

	private static String buildDoubleCondition(String dcol, double[] dvals) {
		String cond = new String();
		for (int i = 0; i < dvals.length; i++) {
			cond = ManageDB.concatStrs(cond, dcol + " BETWEEN ? AND ?");
			cond = ManageDB.concatStrs(cond, "OR");
		}
		return cond.replaceFirst("OR$", "");
	}

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

	private static String buildRegexCondition(String col, String[] vals) {
		String cond = new String();
		for (int i = 0; i < vals.length; i++) {
			cond = ManageDB.concatStrs(col, "~* ?");
			cond = ManageDB.concatStrs(cond, "OR");
		}
		return cond.replaceFirst("OR$", "");
	}

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

	private static HashMap<String, Integer> intitializeJDBCMap() {
		HashMap<String, Integer> jdbcMap = new HashMap<String, Integer>();
		for (int i = 0; i < DB_TYPES.length; i++)
			jdbcMap.put(DB_TYPES[i], JDBC_TYPES[i]);
		return jdbcMap;
	}

	private static boolean isString(ManageDB md, String col) {
		return "character varying".equals(md.getColumnType(col.toLowerCase()));
	}
}
