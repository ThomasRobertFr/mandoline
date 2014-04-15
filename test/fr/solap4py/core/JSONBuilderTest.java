package fr.solap4py.core;

import static org.junit.Assert.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.mdx.SelectNode;

public class JSONBuilderTest {
	private Solap4py solap4py;
	private OlapConnection olapConnection;
	private JSONObject inputTest;
	private SelectNode selectNodeTest;
	private OlapStatement os;
	private CellSet cellSetTest;

	@Before
	public void setUp() throws Exception {
		solap4py = Solap4py.getSolap4Object();
		olapConnection = solap4py.getOlapConnection();
		String s = "{"
				+ "onColumns:"
				+ "["
				+ "\"[Measures].[Goods Quantity]\","
				+ "\"[Measures].[Max Quantity]\""
				+ "],"
				+ " onRows:"
				+ "{"
				+ "\"[Time]\":{\"members\":[\"[Time].[2000]\"],\"range\":false} "
				+ "},"
				+ " where:"
				+ "{"
				+ "\"[Zone.Name]\":{\"members\":[\"[Zone.Name].[France]\"],\"range\":false} "
				+ "}," + "from:" + "\"[Traffic]\"" + "}";
		inputTest = new JSONObject(s);
		selectNodeTest = MDXBuilder.createSelectNode(olapConnection, inputTest);
		os = olapConnection.createStatement();
		cellSetTest = os.executeOlapQuery(selectNodeTest);
	}

	@After
	public void tearDown() throws Exception {
		olapConnection.close();
	}

	@Test
	public void testCreateJSONResponse() throws OlapException, JSONException {

		JSONArray jsonArrayTest = JSONBuilder.createJSONResponse(cellSetTest);
		JSONObject r1 = jsonArrayTest.getJSONObject(0);
		JSONObject r2 = jsonArrayTest.getJSONObject(1);

		assertTrue("The first dimension does not correspond",
				r1.has("[Measures].[Goods Quantity]") && r1.has("[Time]"));
		assertTrue("The second dimension does not correspond",
				r2.has("[Measures].[Max Quantity]") && r2.has("[Time]"));

	}

}
