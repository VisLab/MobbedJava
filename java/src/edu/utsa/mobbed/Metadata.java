package edu.utsa.mobbed;

import java.sql.Connection;
import java.util.UUID;

/**
 * Stores metadata information associated with a dataset.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class Metadata {

	/**
	 * A Attributes object used to store attributes
	 */
	private Attributes atb;
	/**
	 * The dataset UUID of the attribute
	 */
	private UUID datasetUuid;
	/**
	 * A connection to the database
	 */
	private Connection dbCon;

	/**
	 * Creates a Metadata object.
	 * 
	 * @param dbCon
	 *            a connection to the database
	 */
	public Metadata(Connection dbCon) {
		this.dbCon = dbCon;
	}

	/**
	 * Add the attribute to a batch.
	 * 
	 * @param path
	 *            the path of the attribute
	 * @param numericValues
	 *            the numeric values of the attribute
	 * @param values
	 *            the string values of the attribute
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void addAttribute(String path, Double[] numericValues,
			String[] values) throws MobbedException {
		for (int i = 0; i < numericValues.length; i++) {
			atb.reset(UUID.randomUUID(), null, null, datasetUuid, path,
					numericValues[i], values[i]);
			atb.addToBatch();
		}
	}

	/**
	 * Sets the class fields of a Metadata object.
	 * 
	 * @param datasetUuid
	 *            the dataset UUID of the metadata
	 * @throws MobbedException
	 */
	public void reset(String datasetUuid) throws MobbedException {
		this.datasetUuid = UUID.fromString(datasetUuid);
		atb = new Attributes(dbCon);
	}

	/**
	 * Saves all metadata as a batch. All metadata in the batch will be
	 * successfully written or the operation will be aborted.
	 * 
	 * @throws MobbedException
	 *             if an error occurs
	 */
	public void save() throws MobbedException {
		atb.save();
	}

}
