/**
 * @author Cindy Roullet
 * @author Ibrahim Daoudi
 * @version 1.00
 */
package fr.mandoline.core;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.Axis;
import org.olap4j.OlapConnection;
import org.olap4j.impl.IdentifierParser;
import org.olap4j.mdx.*;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

final class MDXBuilder {
	private static final String ON_COLS = "onColumns";
	private static final String ON_ROWS = "onRows";
	private static final String WHERE = "where";
	private static final String FROM = "from";
	private static final String MEMBERS = "members";
	private static final String CUBE_NOT_SPECIFIED = "Cube not specified";

	MDXBuilder() {
	}

	/**
	 * Creates a MDX request inside a SelectNode.
	 *
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param json
	 *            JSONObject containing the request from which we want to create
	 *            the selectNode.
	 * @return The selectNode created from the request contained in the
	 *         parameter json.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	static SelectNode createSelectNode(OlapConnection olapConnection, JSONObject json) throws MandolineException {

		SelectNode selectNodeRequest = initSelectNode(olapConnection, json);

		try {
			if (json.has(ON_COLS)) {
				JSONArray onColumnsJSON = json.getJSONArray(ON_COLS);
				setColumns(onColumnsJSON, selectNodeRequest);
			} else {
				setColumns(new JSONArray(), selectNodeRequest);
			}

			if (json.has(ON_ROWS) && json.getJSONObject(ON_ROWS).length() > 0) {
				JSONObject onRowsJSON = json.getJSONObject(ON_ROWS);
				setRows(onRowsJSON, selectNodeRequest);
			}

			if (json.has(WHERE)) {
				JSONObject whereJSON = json.getJSONObject(WHERE);
				setWhere(whereJSON, selectNodeRequest);
			}

		} catch (JSONException je) {
			throw new MandolineException(ErrorType.BAD_REQUEST, je);
		}
		// solapExeption will be caught in the function execute()

		return selectNodeRequest;
	}

	/**
	 *
	 * Defines the clause where of the MDX request.
	 *
	 * @param whereJSON
	 *            JSONObject containing either data from the key "where".
	 * @param selectNodeRequest
	 *            selectNode on which the Axis FILTER is being set.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setWhere(JSONObject whereJSON, SelectNode selectNodeRequest) throws MandolineException {
		setRowsOrWhere(whereJSON, selectNodeRequest, false);
	}

	/**
	 * Defines the clause on rows of the MDX request.
	 *
	 * @param rowsJSON
	 *            JSONObject containing either data from the key "onRows".
	 * @param selectNodeRequest
	 *            selectNode on which the Axis ROWS is being set.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setRows(JSONObject rowsJSON, SelectNode selectNodeRequest) throws MandolineException {
		setRowsOrWhere(rowsJSON, selectNodeRequest, true);
	}

	/**
	 * Initialize a SelectNode with only the clause from specified.
	 *
	 * @param olapConnection
	 *            Connection to the OLAP database.
	 * @param json
	 *            JSONObject containing the request from which we want to create
	 *            the selectNode.
	 * @return The selectNode initialize with its Cube and COLUMNS empty.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static SelectNode initSelectNode(OlapConnection olapConnection, JSONObject json) throws MandolineException {
		MdxParserFactory pFactory = olapConnection.getParserFactory();
		MdxParser parser = pFactory.createMdxParser(olapConnection);

		String cubeName = getJSONCubeName(json);
		return parser.parseSelect("SELECT {} on COLUMNS FROM " + cubeName);
	}

	/**
	 * Returns the cube's name from the json request.
	 * @param json
	 *            JSONObject containing the request from which we want to create
	 *            the selectNode.
	 * @return The cube's name.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static String getJSONCubeName(JSONObject json) throws MandolineException {
		String cubeJSON;
		try {
			if (json.get(FROM) instanceof String) {
				cubeJSON = json.getString(FROM);
				if ("null".equals(cubeJSON) || cubeJSON == null || "".equals(cubeJSON) || "\"null\"".equals(cubeJSON)) {
					throw new MandolineException(ErrorType.BAD_REQUEST, CUBE_NOT_SPECIFIED);
				}
			} else {
				throw new MandolineException(ErrorType.BAD_REQUEST, CUBE_NOT_SPECIFIED);
			}
		} catch (JSONException e) {
			throw new MandolineException(ErrorType.BAD_REQUEST, e);
		}

		return cubeJSON;
	}

	/**
	 * Defines the clause on columns of the MDX request.
	 *
	 * @param jsonArrayColumns
	 *            JSONOArray containing data from the key "onColumns".
	 * @param selectNode
	 *            selectNode on which the Axis COLUMNS is being set according to
	 *            the parameter jsonArrayColumns.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setColumns(JSONArray jsonArrayColumns, SelectNode selectNode) throws MandolineException {
		List<ParseTreeNode> nodes = new ArrayList<>();

		if (jsonArrayColumns.length() == 0) {
			nodes.add(IdentifierNode.parseIdentifier("[Measures]"));
		} else {
			for (int i = 0; i < jsonArrayColumns.length(); i++) {
				try {
					nodes.add(IdentifierNode.parseIdentifier(jsonArrayColumns.getString(i)));
				} catch (JSONException e) {
					throw new MandolineException(ErrorType.BAD_REQUEST, e);
				}
			}
		}

		CallNode callNodeColumns = new CallNode(null, "{}", Syntax.Braces, nodes);

		selectNode.getAxisList().get(Axis.COLUMNS.axisOrdinal()).setExpression(callNodeColumns);

	}

	/**
	 * Defines the clause on rows or where of the MDX request.
	 *
	 * @param objectJSON
	 *            data containing either data from the key "onRows" or "where"
	 * @param selectNode
	 *            the selectNode on which the Axis FILTER or ROWS will be set
	 *            according to the data in objectJSON.
	 * @param onRows
	 *            boolean that specifies whether the Axis ROWS or FILTER is
	 *            being set in the function.
	 * @throws MandolineException
	 *             Exception that is thrown if the request in objectJSON is bad.
	 */
	private static void setRowsOrWhere(JSONObject objectJSON, SelectNode selectNode, boolean onRows) throws MandolineException {

		ParseTreeNode previous = null;
		ParseTreeNode current;
		ParseTreeNode aggregation;

		Iterator<?> it = objectJSON.keys();
		try {
			while (it.hasNext()) {
				String key = it.next().toString();
				JSONObject hierarchyJSON = objectJSON.getJSONObject(key);
				IdentifierNode identifierWithMemberNode = null;

				/* Create a calculated with member if necessary */
				if (!hierarchyJSON.getBoolean("dice")) {
					JSONArray members = hierarchyJSON.getJSONArray(MEMBERS);

					ParseTreeNode nodeForDice;
					if (hierarchyJSON.getBoolean("range")) {
						if (members.length() == 2) {
							nodeForDice = new CallNode(null, ":", Syntax.Infix,
								IdentifierNode.parseIdentifier(members.getString(0)),
								IdentifierNode.parseIdentifier(members.getString(1))
							);
						} else {
							throw new MandolineException(ErrorType.BAD_REQUEST, "If range is true, two members are required.");
						}
					} else {
						JSONArray membersArray = hierarchyJSON.getJSONArray(MEMBERS);
						List<ParseTreeNode> nodes = new ArrayList<>();

						for (int i = 0; i < membersArray.length(); i++) {
							nodes.add(IdentifierNode.parseIdentifier(membersArray.getString(i)));
						}
						nodeForDice = new CallNode(null, "{}", Syntax.Braces, nodes);
					}

					/* Prepare aggregation name */
					identifierWithMemberNode = IdentifierNode.parseIdentifier(key);
					identifierWithMemberNode = identifierWithMemberNode.append(IdentifierParser.parseIdentifier("[Aggregation]").get(0));


					aggregation = new WithMemberNode(null, identifierWithMemberNode,
						new CallNode(null, "Aggregate", Syntax.Function, nodeForDice),
						Collections.<PropertyValueNode>emptyList()
					);
					selectNode.getWithList().add(aggregation);
				}

				/* Create the "on columns" element */
				if (hierarchyJSON.getBoolean("range") && !hierarchyJSON.getBoolean("dice")) {
					JSONArray members = hierarchyJSON.getJSONArray(MEMBERS);
					if (members.length() == 2) {
						current = new CallNode(null, ":", Syntax.Infix,
							IdentifierNode.parseIdentifier(members.getString(0)),
							IdentifierNode.parseIdentifier(members.getString(1))
						);
					} else {
						throw new MandolineException(ErrorType.BAD_REQUEST, "If range is true, two members are required.");
					}
				} else {
					if (!hierarchyJSON.getBoolean("dice")) {
						current = new CallNode(null, "{}", Syntax.Braces, identifierWithMemberNode);
					} else {
						JSONArray membersArray = hierarchyJSON.getJSONArray(MEMBERS);
						List<ParseTreeNode> nodes = new ArrayList<>();

						for (int i = 0; i < membersArray.length(); i++) {
							nodes.add(IdentifierNode.parseIdentifier(membersArray.getString(i)));
						}
						current = new CallNode(null, "{}", Syntax.Braces, nodes);
					}
				}
				if (previous != null) {
					current = new CallNode(null, "crossjoin", Syntax.Function, current, previous);
				}
				previous = current.deepCopy();
			}
			if (onRows) {
				selectNode.getAxisList().add(new AxisNode(null, false, Axis.ROWS, null, previous));
			} else {
				selectNode.getFilterAxis().setExpression(previous);

			}
		} catch (JSONException je) {
			throw new MandolineException(ErrorType.BAD_REQUEST, je);
		}

		System.out.println(selectNode.toString());
	}
}
