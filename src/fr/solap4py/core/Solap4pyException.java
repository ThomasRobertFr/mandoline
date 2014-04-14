package fr.solap4py.core;


enum ErrorType {
    BAD_REQUEST,
    NOT_SUPPORTED,
    SERVER_ERROR,
    NO_HIERARCHY,
    DIMENSION_ID_COUNT,
    UNKNOWN_ERROR
};

@SuppressWarnings("serial")
public class Solap4pyException extends Exception {

    private final String DESCRIPTION;
    private final ErrorType TYPE;

    public Solap4pyException(ErrorType type, String description) {
        super(description);
        this.TYPE = type;
        this.DESCRIPTION = description == null ? "null" : description;

    }

    public Solap4pyException(ErrorType type, Exception exception) {
        this(type, exception.getMessage());
    }

//    public JSONObject getJSON() throws JSONException {
//        JSONObject objectJson = new JSONObject();
//        objectJson.put("error", TYPE.toString());
//        objectJson.put("data", DESCRIPTION);
//        return objectJson;
//    }
    
    public String getJSON() {
	return "{\"error\":\"" + TYPE.toString() + "\",\"data\":\"" + DESCRIPTION + "\"}"; 
    }

}
