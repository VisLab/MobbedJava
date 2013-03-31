package edu.utsa.mobbed;

public class MobbedException extends Exception {

	private static final long serialVersionUID = 1L;
	private String message;

	public MobbedException(String message) {
		this.message = message;
	}

	public String toString() {
		return this.message;
	}

}
