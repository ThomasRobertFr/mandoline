/**
 * @author Ibrahim Daoudi
 * @author Rémy Chevalier
 * @author Pierre Depeyrot
 * @version 1.02
 */
package fr.solap4py.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
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
	private static final Logger LOGGER = Logger.getLogger(Solap4py.class
			.getName());

	public Solap4py(String host, String port, String user, String passwd,
			String driverName) throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		Connection connection = DriverManager
				.getConnection("jdbc:xmla:Server=http://" + user + ":" + passwd
						+ "@" + host + ":" + port + "/geomondrian/xmla");
		this.olapConnection = connection.unwrap(OlapConnection.class);
		this.metadata = new Metadata(this.olapConnection);
	}

	/**
	 * Accessor to the olapConnection attribute
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

			/* Here, we process the query */
			JSONObject jsonQuery = new JSONObject(query);
			String function = jsonQuery.getString("queryType");
			JSONObject jsonResult = new JSONObject();
			jsonResult.put("error", "OK");

			if ("data".equals(function)) {
				jsonResult
						.put("data", execute(jsonQuery.getJSONObject("data")));
			} else {
				if ("metadata".equals(function)) {
					this.explore(jsonQuery.getJSONObject("data"), jsonResult);
				} else {
					throw new Solap4pyException(ErrorType.NOT_SUPPORTED,
							"The query type " + function
									+ " is not currently supported.");
				}
			}
			result = jsonResult.toString();

		} catch (Solap4pyException se) {
			result = se.getJSON();
		}

		return result;
	}
/*
	private void fonction(){
    	
    	catch (JSONException | OlapException je) {
            LOGGER.log(Level.SEVERE, je.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, je);
        }
    }*/

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
	private void explore(JSONObject query, JSONObject result) throws Solap4pyException {
		
			this.metadata.query(query, result);
	
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
	private JSONArray execute(JSONObject jsonObject) throws Solap4pyException,
			JSONException {
		JSONArray result = new JSONArray();

		SelectNode sn = MDXBuilder.createSelectNode(olapConnection, jsonObject);

		try {
			OlapStatement os = olapConnection.createStatement();
			CellSet cellSet = os.executeOlapQuery(sn);
			result = JSONBuilder.createJSONResponse(cellSet);
		} catch (OlapException oe) {
			throw new Solap4pyException(ErrorType.SERVER_ERROR,
					"Impossible to execute the query");
		}

		return result;
	}

	/**
	 * Returns a Solap4py object initialize with a properties file.
	 * 
	 * @return Solap4py object
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static Solap4py getSolap4Object() throws ClassNotFoundException,
			SQLException {
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
			String driverName = prop.getProperty("driverName");

			return new Solap4py(dbhost, dbport, dbuser, dbpasswd, driverName);
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

	public static void main(String[] args) {
		ServerSocket server = null;
		try {
			final Solap4py solap4py = Solap4py.getSolap4Object();
			server = new ServerSocket(25335);
			System.out.println("Solap4py-java Server started.");
			while (true) {
				try {
					final Socket client = server.accept();
					new Thread() {
						@Override
						public void run() {
							try {
								LOGGER.log(Level.INFO, "connection accepted");
								BufferedReader in = new BufferedReader(
										new InputStreamReader(
												client.getInputStream()));
								PrintWriter out = new PrintWriter(
										client.getOutputStream());
								String query = in.readLine();
								String result = solap4py.process(query);
								out.print(result);
								out.flush();
								client.close();
							} catch (Exception e) {
								LOGGER.log(Level.SEVERE, e.getMessage());
							}
						}
					}.start();
				} catch (IOException acceptClientException) {
					LOGGER.log(Level.SEVERE, acceptClientException.getMessage());
				}
			}
		} catch (ClassNotFoundException | SQLException | IOException initException) {
			LOGGER.log(Level.SEVERE, initException.getMessage());
		} finally {
			if (server != null) {
				try {
					server.close();
				} catch (IOException socketNotFoundException) {
					LOGGER.log(Level.SEVERE,
							socketNotFoundException.getMessage());
				}
			}
		}
	}
}
