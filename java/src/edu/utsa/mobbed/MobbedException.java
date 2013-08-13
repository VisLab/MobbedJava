package edu.utsa.mobbed;

/**
 * Handles exceptions thrown in Mobbed. MobbedException extends Exception.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class MobbedException extends Exception {

	/**
	 * The exception message
	 */
	private String message;
	/**
	 * The serial version UID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a MobbedException object
	 * 
	 * @param message
	 *            - the message of the exception
	 */
	public MobbedException(String message) {
		this.message = message;
	}

	/**
	 * Gets the message
	 */
	public String toString() {
		return this.message;
	}

}
