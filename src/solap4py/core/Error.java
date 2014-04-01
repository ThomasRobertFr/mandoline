package solap4py.core;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

enum ErrorType {
	BAD_REQUEST, 
	NOT_SUPPORTED, 
	SERVER_ERROR, 
	NO_HIERARCHY,
	DIMENSION_ID_COUNT
};

@SuppressWarnings("serial")
public class Error extends Exception {

	private String description;
	private ErrorType type;

	public Error(ErrorType type, String description) {
		super(description);
		this.type = type;
		this.description = description == null ? new String("null") : description;

	}

	public JsonObject getJSON() {

		JsonObjectBuilder objectJsonBuilder = Json.createObjectBuilder();
				
		objectJsonBuilder.add("error", type.toString());
		objectJsonBuilder.add("data", description);
		
		JsonObject objectJson = objectJsonBuilder.build();

		return objectJson;

	}

	public static void controle() throws Error {
		throw new Error(ErrorType.BAD_REQUEST, "Error description");
	}



}