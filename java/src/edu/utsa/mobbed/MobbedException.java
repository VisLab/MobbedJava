package edu.utsa.mobbed;


/**
 * Handles exceptions thrown in Mobbed. MobbedException extends Exception.
 * 
 * @author Arif Hossain, Jeremy Cockfield, Kay Robbins
 * 
 */
public class MobbedException extends Exception {

	private static final long serialVersionUID = 1L;
	private String message;

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
