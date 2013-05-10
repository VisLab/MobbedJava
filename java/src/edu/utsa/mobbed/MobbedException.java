package edu.utsa.mobbed;

/**
 * Handles exceptions thrown in Mobbed.
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
