/**
 * @author Ibrahim Daoudi
 * @author Rémy Chevalier
 * @author Pierre Depeyrot
 * @version 1.02
 */
package fr.mandoline.core;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.mdx.SelectNode;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Mandoline {
	private OlapConnection olapConnection;
	private Metadata metadata;
	private static final Logger LOGGER = Logger.getLogger(Mandoline.class.getName());

	/**
	 * Create a mandoline object connected to a given GeoMondrian
	 *
	 * @param String GeoMondrian's hostname or ip
	 * @param String Port on which GeoMondrian listens
	 * @param String Name of the GeoMondrian webapp
	 *
	 */
	public Mandoline(String host, String port, String geomondrianName) throws SQLException, MandolineException {
		Connection connection = DriverManager
			.getConnection("jdbc:xmla:Server=http://"+host +":"+port+"/"+geomondrianName+"/xmla");

		this.olapConnection = connection.unwrap(OlapConnection.class);
		this.metadata = new Metadata(this.olapConnection);
	}

	/**
	 * Create a mandoline object connected to a given GeoMondrian and bound to a given role
	 *
	 * @param String GeoMondrian's hostname or ip
	 * @param String Port on which GeoMondrian listens
	 * @param String Name of the GeoMondrian webapp
	 * @param String The name of the GeoMondrian role associated with this mandoline
	 *  this is used to handle permissions.
	 *
	 */
	public Mandoline(String host, String port, String geomondrianName, String role) throws SQLException, MandolineException {
		try {
			Connection connection = DriverManager
				.getConnection("jdbc:xmla:Server=http://"+host +":"+port+"/"+geomondrianName+"/xmla");

			this.olapConnection = connection.unwrap(OlapConnection.class);
			this.olapConnection.setRoleName(role);
			this.metadata = new Metadata(this.olapConnection);
		} catch (OlapException e) {
			throw new MandolineException(ErrorType.BAD_ROLE, "The provided role does not exist in GeoMondrian.");
		}
	}

	/**
	 * Process a query in a JSON format
	 * 
	 * @param jsonObject
	 *            The JSONObject representing the query
	 * @return the result of the query in JSON format
	 */
	public String process(JSONObject query) throws MandolineException {
		String result;
		/* Here, we process the query */
		String function = query.getString("queryType");
		JSONObject jsonResult = new JSONObject();
		jsonResult.put("error", "OK");

		if ("data".equals(function)) {
			jsonResult
					.put("data", execute(query.getJSONObject("data")));
		} else {
			if ("metadata".equals(function)) {
				this.explore(query.getJSONObject("data"), jsonResult);
			} else {
				throw new MandolineException(ErrorType.NOT_SUPPORTED,
						"The query type " + function
								+ " is not currently supported.");
			}
		}
		result = jsonResult.toString();

		return result;
	}

	/**
	 * Execute a query to select metadata.
	 * 
	 * @param jsonObject
	 *            a JSON object which indicates which metadata we want to get
	 * @return the result of the query in JSON format
	 * @throws JSONException
	 * @throws MandolineException
	 * @throws OlapException
	 */
	private void explore(JSONObject query, JSONObject result) throws MandolineException {
		this.metadata.query(query, result);
	}

	/**
	 * Execute a query to select data.
	 * 
	 * @param jsonObject
	 *            a JSON text which indicates which data we want to select
	 * @return the result of the query in JSON format
	 * @throws MandolineException
	 * @throws JSONException
	 */
	private JSONArray execute(JSONObject jsonObject) throws MandolineException, JSONException {
		JSONArray result = new JSONArray();

		SelectNode sn = MDXBuilder.createSelectNode(olapConnection, jsonObject);

		try {
			OlapStatement os = olapConnection.createStatement();
			CellSet cellSet = os.executeOlapQuery(sn);
			result = JSONBuilder.createJSONResponse(cellSet);
		} catch (OlapException oe) {
			throw new MandolineException(ErrorType.SERVER_ERROR, "Impossible to execute the query");
		}

		return result;
	}
}
