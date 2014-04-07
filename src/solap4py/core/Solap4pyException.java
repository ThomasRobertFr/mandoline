package solap4py.core;

import org.json.JSONException;
import org.json.JSONObject;

enum ErrorType {
    BAD_REQUEST,
    NOT_SUPPORTED,
    SERVER_ERROR,
    NO_HIERARCHY,
    DIMENSION_ID_COUNT
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

    public JSONObject getJSON() throws JSONException {
        JSONObject objectJson = new JSONObject();
        objectJson.put("error", TYPE.toString());
        objectJson.put("data", DESCRIPTION);
        return objectJson;
    }

}
