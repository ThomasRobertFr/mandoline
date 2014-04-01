package solap4py.core;

import java.io.StringReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;
import org.olap4j.query.Query;
import org.olap4j.query.QueryDimension;


public class Solap4py {

	private OlapConnection olapConnection;
	private Catalog catalog;
	
	public Solap4py(String host, String port, String user, String passwd) {
		try {
			Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
			Connection connection = DriverManager
					.getConnection("jdbc:xmla:Server=http://" + user + ":" + passwd + "@" + host + ":" + port + "/geomondrian/xmla");
			this.olapConnection = connection.unwrap(OlapConnection.class);
			this.catalog = olapConnection.getOlapCatalog();

		} catch (ClassNotFoundException e) {
			System.err.println(e);
		} catch (SQLException e) {
			System.err.println(e);
		}

	}

	public String select(String input) {
		String result = null;
		JsonObject inputJson = Json.createReader(new StringReader(input)).readObject();
		
		Schema schema = null;
		Cube cubeObject = null;
		
		Query myQuery = null;

		try {
			try {
				// If not, result stays "null"
				if (inputJson.containsKey("schema")) {
					// If not, result stays "null"
					if (inputJson.containsKey("cube")) {
						schema = this.catalog.getSchemas().get(inputJson.getString("schema"));
						JsonObject cubeJson = inputJson.getJsonObject("cube");
						JsonArray measuresJson;
						if (cubeJson.containsKey("name") && schema.getCubes().get(cubeJson.getString("name")) != null) {
							// Get the Cube object (Olap4J) associated with this name
							cubeObject = schema.getCubes().get(cubeJson.getString("name"));
							// Initialize the query to be executed
							myQuery = new Query("Select Query", cubeObject);

							
							if (cubeJson.containsKey("measures")) {
								measuresJson = cubeJson.getJsonArray("measures");
								// Measures from array
								QueryDimension measuresDim = myQuery.getDimension("Measures");
								// Put the "Measures" dimension on columns of the expected result
								myQuery.getAxis(Axis.COLUMNS).addDimension(measuresDim);
								
								// Add each measures on columns
								System.out.println(myQuery.getSelect().toString());
								for (JsonValue measureJson : measuresJson) {
									System.out.println(cubeObject.lookupMember(IdentifierNode.ofNames("Measures", measureJson.toString()).getSegmentList()));
									myQuery.getDimension("Measures").include(cubeObject.lookupMember(IdentifierNode.ofNames("Measures", measureJson.toString()).getSegmentList()));
									System.out.println("passed");
								}
							} else {
								throw new Error(ErrorType.BAD_REQUEST, "No measure specified");
							}
							
							// TODO by Pierre.
							if (cubeJson.containsKey("dimension")){
								// Not implemented
								selectDimension(cubeJson.getJsonObject("dimension"), cubeObject, myQuery);
							} else {
								// All the dimensions are aggregated
							}
						}
						else {
							throw new Error(ErrorType.BAD_REQUEST, "Valid cube name not specified");
						}
						
					
					}
				}
			} 
			catch (OlapException olapEx) {
				throw new Error(ErrorType.SERVER_ERROR, olapEx.getMessage());
			}
			catch (SQLException sqlEx) {
				throw new Error(ErrorType.SERVER_ERROR, sqlEx.getMessage());
			}
			catch(Exception ex){
				throw new Error(ErrorType.SERVER_ERROR, ex.getMessage());
			}
		} catch (Error err) {
			result = err.getJSON().toString();
		}
		
		return result;
	}

	private void selectDimension(JsonObject dimension, Cube cubeObject, Query myQuery) throws Error {
		String dimensionName;
		try {
			dimensionName = dimension.getString("name");
			QueryDimension dimObject = myQuery.getDimension(dimensionName);
			// Put the new dimension on rows of the expected result
			myQuery.getAxis(Axis.ROWS).addDimension(dimObject);
			boolean range;
			try {
				range = dimension.getBoolean("range");
			} catch (JsonException e) {
				range = false;
			}
			
			JsonArray ids;
			try {
				String hierarchyName;
				NamedList<Hierarchy> allHierarchies = dimObject.getDimension().getHierarchies();
				Hierarchy hierarchyObject;
				try {
					hierarchyName = dimension.getString("hierarchy");
					hierarchyObject = allHierarchies.get(hierarchyName);
				} catch (Exception e) {
					if (allHierarchies.isEmpty())
						throw new Error(ErrorType.NO_HIERARCHY, new String(
								"No Hierarchy can be found in ").concat(dimensionName)
								.concat(" dimension"));
					else{
						hierarchyObject = dimObject.getDimension().getDefaultHierarchy();
						hierarchyName = hierarchyObject.getName();
					}
				}
				
				
				ids = dimension.getJsonArray("id");
				if (range && ids.size() != 2)
					throw new Error(ErrorType.DIMENSION_ID_COUNT,
							"there should be 2 ID because of range = true");
				else if(range && ids.size() == 2){
					// TODO todo
				}
				else if(!range && ids.isEmpty()){
					try {
						if(dimensionName.equals(hierarchyName)){
							myQuery.getDimension(dimensionName).include(cubeObject.lookupMember(IdentifierNode.ofNames(dimensionName).getSegmentList()));
						}
						else{
							myQuery.getDimension(dimensionName).include(cubeObject.lookupMember(IdentifierNode.ofNames(dimensionName, hierarchyName).getSegmentList()));
						}
					} catch (OlapException e) {
						throw new Error(ErrorType.SERVER_ERROR, e.getMessage()); 
					}
				}
				else{
					// Add each id on rows
					for (JsonValue idJson : ids) {
						try {
							if(dimensionName.equals(hierarchyName)){
								myQuery.getDimension(dimensionName).include(cubeObject.lookupMember(IdentifierNode.ofNames(dimensionName, idJson.toString()).getSegmentList()));
							}
							else{
								myQuery.getDimension(dimensionName).include(cubeObject.lookupMember(IdentifierNode.ofNames(dimensionName, hierarchyName, idJson.toString()).getSegmentList()));
							}
						}
					/*	catch (OlapException err) {
							throw new Error(ErrorType.SERVER_ERROR, err.getMessage()); 
						}*/
						catch (Exception err) {
							throw new Error(ErrorType.SERVER_ERROR, err.getMessage()); 
						}
					}
				}
			} catch (JsonException e) {
				
			}
			
		} catch (JsonException e) {
			throw new Error(ErrorType.BAD_REQUEST,
					"name of dimension cannot be found");
		}

		
/*
		String aggregation = null;
		try {
			aggregation = dimension.getString("aggregation");
		} catch (JsonException e) {
			aggregation = null;
		}

		boolean measure;
		try {
			measure = dimension.getBoolean("measure");
		} catch (JsonException e) {
			measure = false;
		}
*/
		JsonObject subDimension;
		try {
			subDimension = dimension.getJsonObject("dimension");
			selectDimension(subDimension, cubeObject, myQuery);
		} catch (JsonException e) {
			
		}
		
	}

	/**
	 * Execute a query and format the result to get a normalized JsonObject
	 * @param myQuery query to be executed
	 * @return normalized JsonObject
	 * @throws OlapException  If something goes sour, an OlapException will be thrown to the caller. It could be caused by many things, like a stale connection. Look at the root cause for more details.
	 */
	private JsonObject executeSelect(Query myQuery) throws OlapException {
		CellSet resultCellSet = myQuery.execute();
		
		JsonObjectBuilder result = Json.createObjectBuilder();
		result.add("error", "OK");
		JsonObjectBuilder data = Json.createObjectBuilder();

		// TODO get the data and format the returned Json
		for (Position axis_rows : resultCellSet.getAxes().get(Axis.ROWS.axisOrdinal()).getPositions()) {
			for (Position axis_columns : resultCellSet.getAxes().get(Axis.COLUMNS.axisOrdinal()).getPositions()) {
				// Just an exemple of how we browse the CellSet...
				Cell currentCell = resultCellSet.getCell(axis_rows, axis_columns);
				
			}	
		}
		
		result.add("data", data.build());

		return result.build();
	}
	
	public String getMetadata(String param) throws Exception {

		JsonObject query = Json.createReader(new StringReader(param))
				.readObject();
		Metadata m = new Metadata(this.olapConnection);

		return m.query(query).toString();
	}



	
	
	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		InputStream input = null;
		
		String query = "{ \"from\" : [\"Traffic\", \"Traffic\", \"Zone\", \"Zone.Name\", \"Name1\"], \"get\" : \"property\" }";
		
		try {
			File f1 = new File("config.properties");
			if(f1.exists() && !f1.isDirectory()) {
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
			
			Solap4py p = new Solap4py(dbhost, dbport, dbuser, dbpasswd);
			String metadata = p.getMetadata(query);
			
			System.out.println(metadata);
		
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
		
		
	}

}
