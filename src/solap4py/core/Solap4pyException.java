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

    private String description;
    private ErrorType type;

    public Solap4pyException(ErrorType type, String description) {
        super(description);
        this.type = type;
        this.description = description == null ? new String("null") : description;

    }

    public JSONObject getJSON() throws JSONException {
        JSONObject objectJson = new JSONObject();
        objectJson.put("error", type.toString());
        objectJson.put("data", description);
        return objectJson;
    }

    public static void controle() throws Solap4pyException {
        throw new Solap4pyException(ErrorType.BAD_REQUEST, "Solap4pyException description");
    }

}
