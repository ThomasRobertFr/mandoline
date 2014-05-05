package fr.solap4py.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.olap4j.Axis;
import org.olap4j.OlapConnection;
import org.olap4j.mdx.SelectNode;

public class MDXBuilderTest {

	private Solap4py solap4py;
	private OlapConnection olapConnection;
	private JSONObject inputTest;
	private JSONObject inputTest2;
	private JSONObject inputTest3;

	private Method getJSONCubeName;
	private Method initSelectNode;
	private Method setColumns;
	private Method setRowsOrWhere;
	private Method setRows;
	private Method setWhere;

	private JSONArray columnsTest;
	private JSONObject rowsTest;
	private JSONObject whereTest;

	private JSONArray columnsTest2;
	private JSONObject rowsTest2;
	private JSONObject whereTest2;

	private JSONArray columnsTest3;
	private JSONObject rowsTest3;
	private JSONObject whereTest3;

	@Before
	public void setUp() throws Exception {
		solap4py = Solap4py.getSolap4Object();
		olapConnection = solap4py.getOlapConnection();

		String sBad = "{" + "onColumns:" + "["
				+ "\"[Measures].[Goods Quantity]\"," + "12" + "]," + " onRows:"
				+ "{" + "\"[Time]\":{\"members\":[\"[2000]\"],\"range\":true} "
				+ "}," + " where:" + "{"
				+ "sqldkfjglqejg:{qrgqf:[qzefqzef],\"range\":false} " + "},"
				+ "from:" + "\"\"" + "}";

		String sBad2 = "{"
				+ "onColumns:"
				+ "["
				+ "\"[Measures].[Goods Quantity]\","
				+ "12"
				+ "],"
				+ " onRows:"
				+ "{"
				+ "1354:{strhsz:[s],\"range\":false} "
				+ "},"
				+ " where:"
				+ "{"
				+ "\"[Zone.Name]\":{\"members\":[\"[France]\"],\"range\":false} "
				+ "}," + "from:" + "\"\"" + "}";

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
		inputTest = new JSONObject(s);
		inputTest2 = new JSONObject(sBad);
		inputTest3 = new JSONObject(sBad2);

		columnsTest = inputTest.getJSONArray("onColumns");
		rowsTest = inputTest.getJSONObject("onRows");
		whereTest = inputTest.getJSONObject("where");

		columnsTest2 = inputTest2.getJSONArray("onColumns");
		rowsTest2 = inputTest2.getJSONObject("onRows");
		whereTest2 = inputTest2.getJSONObject("where");

		columnsTest3 = inputTest3.getJSONArray("onColumns");
		rowsTest3 = inputTest3.getJSONObject("onRows");
		whereTest3 = inputTest3.getJSONObject("where");

		getJSONCubeName = MDXBuilder.class.getDeclaredMethod("getJSONCubeName",
				JSONObject.class);
		getJSONCubeName.setAccessible(true);

		initSelectNode = MDXBuilder.class.getDeclaredMethod("initSelectNode",
				OlapConnection.class, JSONObject.class);
		initSelectNode.setAccessible(true);

		setColumns = MDXBuilder.class.getDeclaredMethod("setColumns",
				JSONArray.class, SelectNode.class);
		setColumns.setAccessible(true);

		setRowsOrWhere = MDXBuilder.class.getDeclaredMethod("setRowsOrWhere",
				JSONObject.class, SelectNode.class, boolean.class);
		setRowsOrWhere.setAccessible(true);

		setRows = MDXBuilder.class.getDeclaredMethod("setRows",
				JSONObject.class, SelectNode.class);
		setRows.setAccessible(true);

		setWhere = MDXBuilder.class.getDeclaredMethod("setWhere",
				JSONObject.class, SelectNode.class);
		setWhere.setAccessible(true);

	}

	@After
	public void tearDown() throws Exception {
		olapConnection.close();
	}


	@Test(expected = Solap4pyException.class)
	public void testGetJSONCubeName() throws Throwable {
		String cubeNameTest;
		try {
			cubeNameTest = (String) (getJSONCubeName.invoke(null, inputTest));

			assertEquals("getJSONCube did not get the cube name properly ",
					"[Traffic]", cubeNameTest);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}

		// thrown.expectMessage("Cube not specified");
		try {
			// thrown.expect(Solap4pyException.class );
			String cubeName2 = (String) (getJSONCubeName.invoke(null,
					inputTest2));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testInitSelectNode() throws Throwable {

		try {
			SelectNode selectNodeTest = (SelectNode) (initSelectNode.invoke(
					null, olapConnection, inputTest));

			assertTrue("initSelectNode did not return a SelectNode",
					selectNodeTest instanceof SelectNode);
			assertEquals("From clause was not corectly set", "[Traffic]",
					selectNodeTest.getFrom().toString());

		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}

		try {
			SelectNode selectNodeTest = (SelectNode) (initSelectNode.invoke(
					null, olapConnection, inputTest2));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testSetColumns() throws Throwable {

		try {
			SelectNode selectNodeTest = (SelectNode) (initSelectNode.invoke(
					null, olapConnection, inputTest));

			String columnsBefore = selectNodeTest.getAxisList()
					.get(Axis.COLUMNS.axisOrdinal()).getExpression().toString();

			setColumns.invoke(null, columnsTest, selectNodeTest);

			String columnsAfter = selectNodeTest.getAxisList()
					.get(Axis.COLUMNS.axisOrdinal()).getExpression().toString();

			assertFalse(
					"setColumns did not modify the columns of the SelectNode",
					columnsAfter.equals(columnsBefore));
			// assert("From clause was not correctly set", "[Traffic]",
			// selectNodeTest.getFrom().toString());

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		SelectNode selectNodeTest2;
		try {
			selectNodeTest2 = (SelectNode) (initSelectNode.invoke(null,
					olapConnection, inputTest));
			setColumns.invoke(null, columnsTest2, selectNodeTest2);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testSetRowsOrWhere() throws Throwable {
		try {
			SelectNode selectNodeTest = (SelectNode) (initSelectNode.invoke(
					null, olapConnection, inputTest));
			setColumns.invoke(null, columnsTest, selectNodeTest);

			setRowsOrWhere.invoke(null, rowsTest, selectNodeTest, true);

			String rowsAfter = selectNodeTest.getAxisList()
					.get(Axis.ROWS.axisOrdinal()).getExpression().toString();

			assertNotNull("ON ROWS did not set", rowsAfter);

			setRowsOrWhere.invoke(null, whereTest, selectNodeTest, false);
			String whereAfter = selectNodeTest.getFilterAxis().getExpression()
					.toString();
			assertNotNull("WHERE did not set", whereAfter);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		SelectNode selectNodeTest;
		try {
			selectNodeTest = (SelectNode) (initSelectNode.invoke(null,
					olapConnection, inputTest));
			setColumns.invoke(null, columnsTest, selectNodeTest);
			setRowsOrWhere.invoke(null, rowsTest2, selectNodeTest, true);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {

			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testSetRows() throws Throwable {
		try {
			SelectNode selectNodeTest = (SelectNode) (initSelectNode.invoke(
					null, olapConnection, inputTest));
			setColumns.invoke(null, columnsTest, selectNodeTest);

			setRows.invoke(null, rowsTest, selectNodeTest);
			String rowsAfter = selectNodeTest.getAxisList()
					.get(Axis.ROWS.axisOrdinal()).getExpression().toString();
			assertNotNull("ON ROWS did not set", rowsAfter);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		SelectNode selectNodeTest;
		try {
			selectNodeTest = (SelectNode) (initSelectNode.invoke(null,
					olapConnection, inputTest));
			setColumns.invoke(null, columnsTest, selectNodeTest);

			setRows.invoke(null, rowsTest3, selectNodeTest);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testSetWhere() throws Throwable {
		try {
			SelectNode selectNodeTest = (SelectNode) (initSelectNode.invoke(
					null, olapConnection, inputTest));
			setColumns.invoke(null, columnsTest, selectNodeTest);
			setRows.invoke(null, rowsTest, selectNodeTest);

			setWhere.invoke(null, whereTest, selectNodeTest);
			String whereAfter = selectNodeTest.getFilterAxis().getExpression()
					.toString();
			assertNotNull("WHERE did not set", whereAfter);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		SelectNode selectNodeTest;
		try {
			selectNodeTest = (SelectNode) (initSelectNode.invoke(null,
					olapConnection, inputTest));
			setColumns.invoke(null, columnsTest, selectNodeTest);
			setRows.invoke(null, rowsTest, selectNodeTest);

			setWhere.invoke(null, whereTest2, selectNodeTest);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test
	public void testCreateSelectNode() {

		try {

			SelectNode selectNodeTest = MDXBuilder.createSelectNode(
					olapConnection, inputTest);

			assertTrue(
					"createSelectNode did not return an object of type SelectNode",
					selectNodeTest instanceof SelectNode);

			String rowsAfter = selectNodeTest.getAxisList()
					.get(Axis.ROWS.axisOrdinal()).getExpression().toString();
			assertNotNull("ON ROWS did not set", rowsAfter);
			String whereAfter = selectNodeTest.getFilterAxis().getExpression()
					.toString();
			assertNotNull("WHERE did not set", whereAfter);

		} catch (Solap4pyException e) {
			e.printStackTrace();
		}

	}

}
