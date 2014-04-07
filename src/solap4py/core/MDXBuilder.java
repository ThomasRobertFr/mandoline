package solap4py.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.olap4j.Axis;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.olap4j.OlapConnection;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.Syntax;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;

public class MDXBuilder {

    static SelectNode createSelectNode(OlapConnection olapConnection, JSONObject json) throws Solap4pyException {

        SelectNode selectNodeRequest = initSelectNode(olapConnection, json);

        try {
            JSONArray onColumnsJSON = json.getJSONArray("onColumns");
            JSONObject onRowsJSON = json.getJSONObject("onRows");
            JSONObject whereJSON = json.getJSONObject("where");

            setColumns(olapConnection, onColumnsJSON, selectNodeRequest);
            setRows(olapConnection, onRowsJSON, selectNodeRequest);
            setWhere(olapConnection, whereJSON, selectNodeRequest);

        } catch (JSONException je) {
            throw new Solap4pyException(ErrorType.BAD_REQUEST, je.getMessage());
        }
        // solapExeption à catcher dans execute

        return selectNodeRequest;

    }

    private static void setWhere(OlapConnection olapConnection, JSONObject whereJSON, SelectNode selectNodeRequest)
                                                                                                                   throws Solap4pyException {
        setRowsOrWhere(olapConnection, whereJSON, selectNodeRequest, false);
    }

    private static void setRows(OlapConnection olapConnection, JSONObject whereJSON, SelectNode selectNodeRequest) throws Solap4pyException {

        setRowsOrWhere(olapConnection, whereJSON, selectNodeRequest, true);
    }

    private static SelectNode initSelectNode(OlapConnection olapConnection, JSONObject json) throws Solap4pyException {
        MdxParserFactory pFactory = olapConnection.getParserFactory();
        MdxParser parser = pFactory.createMdxParser(olapConnection);

        String cubeName = getJSONCubeName(json);
        SelectNode sn = parser.parseSelect("SELECT {} on COLUMNS FROM " + cubeName);
        return sn;
    }

    private static String getJSONCubeName(JSONObject json) throws Solap4pyException {
        // TODO Auto-generated method stub
        String cubeJSON;
        try {
            cubeJSON = json.getString("from");
        } catch (JSONException e) {
            System.err.println(e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "From unreachable.");

        }

        return cubeJSON.toString();
    }

    private static void setColumns(OlapConnection olapConnection, JSONArray jsonArayColumns, SelectNode selectNode)
                                                                                                                   throws Solap4pyException {
        List<ParseTreeNode> nodes = new ArrayList<ParseTreeNode>();

        for (int i = 0; i < jsonArayColumns.length(); i++) {

            try {
                nodes.add(IdentifierNode.parseIdentifier(jsonArayColumns.getString(i)));
            } catch (JSONException e) {
                throw new Solap4pyException(ErrorType.BAD_REQUEST, e.getMessage());
            }
        }

        CallNode callNodeColumns = new CallNode(null, "{}", Syntax.Braces, nodes);

        selectNode.getAxisList().get(Axis.COLUMNS.axisOrdinal()).setExpression(callNodeColumns);

    }

    private static void setRowsOrWhere(OlapConnection olapConnection, JSONObject objectJSON, SelectNode selectNode, boolean onRows)
                                                                                                                                   throws Solap4pyException {

        ParseTreeNode previous = null;
        ParseTreeNode current = null;

        Iterator<String> it = objectJSON.keys();
        try {
            while (it.hasNext()) {
                String key = it.next();
                JSONObject hierarchyJSON = objectJSON.getJSONObject(key);
                if ((boolean) hierarchyJSON.get("range")) {
                    JSONArray members = hierarchyJSON.getJSONArray("members");
                    if (members.length() == 2) {
                        current = new CallNode(null, ":", Syntax.Infix, IdentifierNode.parseIdentifier(key + "." + members.getString(0)),
                                               IdentifierNode.parseIdentifier(key + "." + members.getString(1)));
                    } else {
                        throw new Solap4pyException(ErrorType.BAD_REQUEST, "If range is true, two members are required.");
                    }
                } else {
                    JSONArray membersArray = hierarchyJSON.getJSONArray("members");
                    List<ParseTreeNode> nodes = new ArrayList<ParseTreeNode>();

                    for (int i = 0; i < membersArray.length(); i++) {

                        nodes.add(IdentifierNode.parseIdentifier(key + "." + membersArray.getString(i)));
                    }
                    current = new CallNode(null, "{}", Syntax.Braces, nodes);
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
            throw new Solap4pyException(ErrorType.BAD_REQUEST, je.getMessage());
        }

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, JSONException, Solap4pyException {
        // try{
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
                + "}," 
                + "from:"
                + "\"[Traffic]\""
                + "}";
        JSONObject inputTest2 = new JSONObject(s);
        System.out.println(inputTest2.toString());

        // Connection to database
        Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
        Connection connection = DriverManager.getConnection("jdbc:xmla:Server=http://postgres:westcoast@192.168.1.1:8080/geomondrian/xmla");
        OlapConnection olapConnection = connection.unwrap(OlapConnection.class);

        // test initSelectNode
        SelectNode selectNodeTest = initSelectNode(olapConnection, inputTest2);
        
        // test setColumns
        JSONArray onColumnsTest = inputTest2.getJSONArray("onColumns");
        setColumns(olapConnection, onColumnsTest, selectNodeTest);
        
        // test setRows
        JSONObject onRowsTest = inputTest2.getJSONObject("onRows");
        setRows(olapConnection, onRowsTest, selectNodeTest);
        
        
     // test setWhere
        JSONObject whereTest = inputTest2.getJSONObject("where");
        setWhere(olapConnection, whereTest, selectNodeTest);
        System.out.println(selectNodeTest.toString());
        
        // test createSelectNode
        
        /*
         * } catch(JSONException e){
         * 
         * System.err.println(e.getMessage()); }
         */

    }

}
