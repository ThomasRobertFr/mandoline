/**
 * @author Cindy Roullet
 * @author Ibrahim Daoudi
 * @author Rémy Chevalier
 * @version 1.00
 */
package fr.mandoline.core;

enum ErrorType {
	BAD_REQUEST,
	NOT_SUPPORTED,
	SERVER_ERROR
	//NO_HIERARCHY,
	//DIMENSION_ID_COUNT,
	//UNKNOWN_ERROR
};

@SuppressWarnings("serial")
public class MandolineException extends Exception {

	private final String description;
	private final ErrorType type;

	public MandolineException(ErrorType type, String description) {
		super(description);
		this.type = type;
		this.description = description == null ? "null" : description;

	}

	public MandolineException(ErrorType type, Exception exception) {
		this(type, exception.getMessage());
	}

	/**
	 *
	 * @return the exception in a String but written like a JSON. With the error type and its description.
	 */
	public String getJSON() {
		return "{\"error\":\"" + type.toString() + "\",\"data\":\"" + description + "\"}";
	}
}
