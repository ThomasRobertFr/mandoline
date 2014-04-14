package fr.solap4py.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.mdx.SelectNode;
import org.olap4j.metadata.Catalog;

public class Solap4py {
    private OlapConnection olapConnection;
    private Catalog catalog;
    private Metadata metadata;

    public Solap4py(String host, String port, String user, String passwd) throws ClassNotFoundException, SQLException {
        try {
            Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
            Connection connection = DriverManager.getConnection("jdbc:xmla:Server=http://" + user + ":" + passwd + "@" + host + ":" + port
                                                                + "/geomondrian/xmla");
            this.olapConnection = connection.unwrap(OlapConnection.class);
            this.catalog = olapConnection.getOlapCatalog();
            this.metadata = new Metadata(this.olapConnection);

        } catch (ClassNotFoundException e) {
            System.err.println(e);
        } catch (SQLException e) {
            System.err.println(e);
        }
    }
    
    /**
     * Accessor to olapConnection attribute 
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
                throw new Solap4pyException(ErrorType.BAD_REQUEST, je);
            }
        } catch (Solap4pyException se) {
            //try {
                result = se.getJSON();
//            } catch (JSONException je) {
//                // We have to catch a JSONException if an error occurred while
//                // formatting the output JSON
//                return "{error: INTERNAL_ERROR, data: An internal error occurred while formatting the output JSON.}";
//            }
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

        System.out.println(sn.toString());
        try {
            OlapStatement os = olapConnection.createStatement();
            CellSet cellSet = os.executeOlapQuery(sn);
            result = JSONBuilder.createJSONResponse(cellSet);
        } catch (OlapException oe) {
            throw new Solap4pyException(ErrorType.SERVER_ERROR, oe);
        }

        return result;
    }
    
    public static Solap4py getSolap4Object() throws ClassNotFoundException, SQLException{
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
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
