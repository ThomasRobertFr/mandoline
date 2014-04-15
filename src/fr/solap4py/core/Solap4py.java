package fr.solap4py.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.mdx.SelectNode;

public class Solap4py {
    private OlapConnection olapConnection;
    private Metadata metadata;
    private static final Logger LOGGER = Logger.getLogger(Solap4py.class.getName());

    public Solap4py(String host, String port, String user, String passwd) throws ClassNotFoundException, SQLException {
        try {
            Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
            Connection connection = DriverManager.getConnection("jdbc:xmla:Server=http://" + user + ":" + passwd + "@" + host + ":" + port
                                                                + "/geomondrian/xmla");
            this.olapConnection = connection.unwrap(OlapConnection.class);
            this.metadata = new Metadata(this.olapConnection);

        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
        }
    }

    /**
     * Accessor to olapConnection attribute
     * 
     * @return the OlapConnection associated with this instance of Solap4py
     */
    public OlapConnection getOlapConnection() {
        return this.olapConnection;
    }

    /**
     * Process a query in a JSON format
     * 
     * @param query
     *            the input query in JSON format
     * @return the result of the query in JSON format
     */
    public String process(String query) {
        String result;

        try {
            try {
                /* Here, we process the query */
                JSONObject jsonQuery = new JSONObject(query);
                String function = jsonQuery.getString("queryType");
                JSONObject jsonResult = new JSONObject();
                jsonResult.put("error", "OK");

                if ("data".equals(function)) {
                    jsonResult.put("data", execute(jsonQuery.getJSONObject("data")));
                } else {
                    if ("metadata".equals(function)) {
                        this.explore(jsonQuery.getJSONObject("data"), jsonResult);
                    } else {
                        throw new Solap4pyException(ErrorType.NOT_SUPPORTED, "The query type " + function + " is not currently supported.");
                    }
                }
                result = jsonResult.toString();
            } catch (JSONException | OlapException je) {
                LOGGER.log(Level.SEVERE, je.getMessage());
                throw new Solap4pyException(ErrorType.BAD_REQUEST, je);
            }
        } catch (Solap4pyException se) {
            result = se.getJSON();
        }

        return result;
    }

    /**
     * Execute a query to select metadata.
     * 
     * @param jsonObject
     *            a JSON object which indicates which metadata we want to get
     * @return the result of the query in JSON format
     * @throws JSONException
     * @throws Solap4pyException
     * @throws OlapException
     */
    private JSONObject explore(JSONObject query, JSONObject result) throws JSONException, Solap4pyException, OlapException {
        return this.metadata.query(query, result);
    }

    /**
     * Execute a query to select data.
     * 
     * @param jsonObject
     *            a JSON text which indicates which data we want to select
     * @return the result of the query in JSON format
     * @throws Solap4pyException
     * @throws JSONException
     */
    private JSONArray execute(JSONObject jsonObject) throws Solap4pyException, JSONException {
        JSONArray result = new JSONArray();

        SelectNode sn = MDXBuilder.createSelectNode(olapConnection, jsonObject);

        try {
            OlapStatement os = olapConnection.createStatement();
            CellSet cellSet = os.executeOlapQuery(sn);
            result = JSONBuilder.createJSONResponse(cellSet);
        } catch (OlapException oe) {
            throw new Solap4pyException(ErrorType.SERVER_ERROR, "Impossible to execute the query");
        }

        return result;
    }

    public static Solap4py getSolap4Object() throws ClassNotFoundException, SQLException {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            File f1 = new File("config.properties");
            if (f1.exists() && !f1.isDirectory()) {
                input = new FileInputStream(f1);
            } else {
                input = new FileInputStream("config.dist");
            }

            // load a properties file
            prop.load(input);

            // get the property value
            String dbhost = prop.getProperty("dbhost");
            String dbuser = prop.getProperty("dbuser");
            String dbpasswd = prop.getProperty("dbpasswd");
            String dbport = prop.getProperty("dbport");

            return new Solap4py(dbhost, dbport, dbuser, dbpasswd);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage());
                }
            }
        }
        return null;
    }
}
