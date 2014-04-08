package fr.solap4py.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.Axis;
import org.olap4j.OlapConnection;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.Syntax;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;

final class MDXBuilder {
	MDXBuilder() {
	}

	/**
	 * 
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param json
	 *            JSONObject containing the request from which we want to create
	 *            the selectNode.
	 * @return The selectNode created from the request contained in the
	 *         parameter json.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	static SelectNode createSelectNode(OlapConnection olapConnection,
			JSONObject json) throws Solap4pyException {

		SelectNode selectNodeRequest = initSelectNode(olapConnection, json);

		try {
			JSONArray onColumnsJSON = json.getJSONArray("onColumns");
			setColumns(onColumnsJSON, selectNodeRequest);
			if (json.has("onRows")) {
				JSONObject onRowsJSON = json.getJSONObject("onRows");
				setRows(olapConnection, onRowsJSON, selectNodeRequest);
			}

			if (json.has("where")) {
				JSONObject whereJSON = json.getJSONObject("where");
				setWhere(olapConnection, whereJSON, selectNodeRequest);
			}

		} catch (JSONException je) {
			throw new Solap4pyException(ErrorType.BAD_REQUEST, je);
		}
		// solapExeption will be caught in the function execute()

		return selectNodeRequest;

	}

	/**
	 * 
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param whereJSON
	 *            JSONObject containing either data from the key "where".
	 * @param selectNodeRequest
	 *            selectNode on which the Axis FILTER is being set.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setWhere(OlapConnection olapConnection,
			JSONObject whereJSON, SelectNode selectNodeRequest)
			throws Solap4pyException {
		setRowsOrWhere(whereJSON, selectNodeRequest, false);
	}

	/**
	 * 
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param rowsJSON
	 *            JSONObject containing either data from the key "onRows".
	 * @param selectNodeRequest
	 *            selectNode on which the Axis ROWS is being set.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setRows(OlapConnection olapConnection,
			JSONObject rowsJSON, SelectNode selectNodeRequest)
			throws Solap4pyException {

		setRowsOrWhere(rowsJSON, selectNodeRequest, true);
	}

	/**
	 * 
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param json
	 *            JSONObject containing the request from which we want to create
	 *            the selectNode.
	 * @return The selectNode initialize with its Cube and COLUMNS empty.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static SelectNode initSelectNode(OlapConnection olapConnection,
			JSONObject json) throws Solap4pyException {
		MdxParserFactory pFactory = olapConnection.getParserFactory();
		MdxParser parser = pFactory.createMdxParser(olapConnection);

		String cubeName = getJSONCubeName(json);
		return parser.parseSelect("SELECT {} on COLUMNS FROM " + cubeName);
	}

	/**
	 * 
	 * @param json
	 *            JSONObject containing the request from which we want to create
	 *            the selectNode.
	 * @return The cube's name.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static String getJSONCubeName(JSONObject json)
			throws Solap4pyException {
		// TODO Auto-generated method stub
		String cubeJSON;
		try {
			cubeJSON = json.getString("from");
		} catch (JSONException e) {
			throw new Solap4pyException(ErrorType.BAD_REQUEST, e);
		}

		return cubeJSON;
	}

	/**
	 * 
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param jsonArrayColumns
	 *            JSONOArray containing data from the key "onColumns".
	 * @param selectNode
	 *            selectNode on which the Axis COLUMNS is being set according to
	 *            the parameter jsonArrayColumns.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setColumns(JSONArray jsonArrayColumns,
			SelectNode selectNode) throws Solap4pyException {
		List<ParseTreeNode> nodes = new ArrayList<ParseTreeNode>();

		for (int i = 0; i < jsonArrayColumns.length(); i++) {

			try {
				nodes.add(IdentifierNode.parseIdentifier(jsonArrayColumns
						.getString(i)));
			} catch (JSONException e) {
				throw new Solap4pyException(ErrorType.BAD_REQUEST, e);
			}
		}

		CallNode callNodeColumns = new CallNode(null, "{}", Syntax.Braces,
				nodes);

		selectNode.getAxisList().get(Axis.COLUMNS.axisOrdinal())
				.setExpression(callNodeColumns);

	}

	/**
	 * 
	 * @param olapConnection
	 *            Connection to the OLAP database
	 * @param objectJSON
	 *            data containing either data from the key "onRows" or "where"
	 * @param selectNode
	 *            the selectNode on which the Axis FILTER or ROWS will be set
	 *            according to the data in objectJSON.
	 * @param onRows
	 *            boolean that specifies whether the Axis ROWS or FILTER is
	 *            being set in the function.
	 * @throws Solap4pyException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setRowsOrWhere(JSONObject objectJSON,
			SelectNode selectNode, boolean onRows) throws Solap4pyException {

		ParseTreeNode previous = null;
		ParseTreeNode current = null;

		Iterator<?> it = objectJSON.keys();
		try {
			while (it.hasNext()) {
				String key = it.next().toString();
				JSONObject hierarchyJSON = objectJSON.getJSONObject(key);
				if ((boolean) hierarchyJSON.get("range")) {
					JSONArray members = hierarchyJSON.getJSONArray("members");
					if (members.length() == 2) {
						current = new CallNode(null, ":", Syntax.Infix,
								IdentifierNode.parseIdentifier(key + "."
										+ members.getString(0)),
								IdentifierNode.parseIdentifier(key + "."
										+ members.getString(1)));
					} else {
						throw new Solap4pyException(ErrorType.BAD_REQUEST,
								"If range is true, two members are required.");
					}
				} else {
					JSONArray membersArray = hierarchyJSON
							.getJSONArray("members");
					List<ParseTreeNode> nodes = new ArrayList<ParseTreeNode>();

					for (int i = 0; i < membersArray.length(); i++) {

						nodes.add(IdentifierNode.parseIdentifier(key + "."
								+ membersArray.getString(i)));
					}
					current = new CallNode(null, "{}", Syntax.Braces, nodes);
				}
				if (previous != null) {
					current = new CallNode(null, "crossjoin", Syntax.Function,
							current, previous);
				}
				previous = current.deepCopy();
			}
			if (onRows) {
				selectNode.getAxisList().add(
						new AxisNode(null, false, Axis.ROWS, null, previous));
			} else {
				selectNode.getFilterAxis().setExpression(previous);

			}
		} catch (JSONException je) {
			throw new Solap4pyException(ErrorType.BAD_REQUEST, je);
		}

	}

	public static void main(String[] args) throws JSONException, IOException,
			ClassNotFoundException, SQLException, Solap4pyException {
		String s = "{"
				+ "onColumns:"
				+ "["
				+ "\"[Measures].[Goods Quantity]\","
				+ "\"[Measures].[Max Quantity]\""
				+ "],"
				+ " onRows:"
				+ "{"
				+ "\"[Time]\":{\"members\":[\"[2000]\"],\"range\":false} "
				+ "},"
				+ " where:"
				+ "{"
				+ "\"[Zone.Name]\":{\"members\":[\"[France]\"],\"range\":false} "
				+ "}," + "from:" + "\"[Traffic]\"" + "}";
		JSONObject inputTest2 = new JSONObject(s);
		System.out.println(inputTest2.toString());

		// Connection to database
		Properties prop = new Properties();
		InputStream input = null;

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

		Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
		Connection connection = DriverManager
				.getConnection("jdbc:xmla:Server=http://" + dbuser + ":"
						+ dbpasswd + "@" + dbhost + ":" + dbport
						+ "/geomondrian/xmla");
		OlapConnection olapConnection = connection.unwrap(OlapConnection.class);

		// test initSelectNode
		SelectNode selectNodeTest = initSelectNode(olapConnection, inputTest2);

		// test setColumns
		JSONArray onColumnsTest = inputTest2.getJSONArray("onColumns");
		setColumns(onColumnsTest, selectNodeTest);

		// test setRows
		JSONObject onRowsTest = inputTest2.getJSONObject("onRows");
		setRows(olapConnection, onRowsTest, selectNodeTest);

		// test setWhere
		JSONObject whereTest = inputTest2.getJSONObject("where");
		setWhere(olapConnection, whereTest, selectNodeTest);
		System.out.println(selectNodeTest.toString());

		// test createSelectNode

		SelectNode selectNodeTest2 = createSelectNode(olapConnection,
				inputTest2);
		System.out.println(selectNodeTest2.toString());

		olapConnection.close();
		connection.close();
		input.close();

	}

}
