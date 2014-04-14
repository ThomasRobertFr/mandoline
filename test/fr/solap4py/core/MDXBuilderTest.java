package fr.solap4py.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.json.JSONArray;
import org.json.JSONException;
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
	
	private Method getJSONCubeName;
	private Method  initSelectNode;
	private Method setColumns;
	private Method setRowsOrWhere;
	private Method setRows;
	private Method setWhere;
	
	private JSONArray columnsTest;
	private JSONObject rowsTest;
	private JSONObject whereTest;
	
	
	@Before
	public void setUp() throws Exception {
		solap4py = Solap4py.getSolap4Object();
		olapConnection = solap4py.getOlapConnection();
		
		String s2 = "{"
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
	inputTest2 = new JSONObject(s2);
	
	columnsTest = inputTest.getJSONArray("onColumns");
	rowsTest = inputTest.getJSONObject("onRows");
	whereTest = inputTest.getJSONObject("where");
	

	
	getJSONCubeName = MDXBuilder.class.getDeclaredMethod("getJSONCubeName", JSONObject.class);
	getJSONCubeName.setAccessible(true); 
	
	initSelectNode = MDXBuilder.class.getDeclaredMethod("initSelectNode", OlapConnection.class,JSONObject.class);
	initSelectNode.setAccessible(true);
	
	setColumns = MDXBuilder.class.getDeclaredMethod("setColumns", JSONArray.class,SelectNode.class);
	setColumns.setAccessible(true);
	
	setRowsOrWhere = MDXBuilder.class.getDeclaredMethod("setRowsOrWhere", JSONObject.class,SelectNode.class,boolean.class);
	setRowsOrWhere.setAccessible(true);
	
	setRows = MDXBuilder.class.getDeclaredMethod("setRows", JSONObject.class,SelectNode.class);
	setRows.setAccessible(true);
	
	setWhere = MDXBuilder.class.getDeclaredMethod("setWhere", JSONObject.class,SelectNode.class);
	setWhere.setAccessible(true);
	
	
	}

	@After
	public void tearDown() throws Exception {
		olapConnection.close();
	}

	
	@Rule public ExpectedException thrown= ExpectedException.none();
	
	@Test (expected = Solap4pyException.class)
	public void testGetJSONCubeName() throws Throwable {
		String cubeNameTest;
		try {
			cubeNameTest = (String)(getJSONCubeName.invoke(null, inputTest));
		
			assertEquals("getJSONCube did not get the cube name properly ","[Traffic]",cubeNameTest);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}	
		
				
					 
			 //thrown.expectMessage("Cube not specified");
			 try {
				 //thrown.expect(Solap4pyException.class );
				String cubeName2 = (String)(getJSONCubeName.invoke(null,inputTest2));
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw e.getCause();
			}
		
		
		
		 
		
		
		
	}
	
	
	
	@Test (expected = Solap4pyException.class)
	public void testInitSelectNode() throws Throwable{
		
		
		try {
			SelectNode selectNodeTest = (SelectNode)(initSelectNode.invoke(null, olapConnection,inputTest));
			
			assertTrue("initSelectNode did not return a SelectNode",selectNodeTest instanceof SelectNode);
			assertEquals("From clause was not corectly set", "[Traffic]", selectNodeTest.getFrom().toString());
			
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}
		
		try {
			SelectNode selectNodeTest = (SelectNode)(initSelectNode.invoke(null, olapConnection,inputTest2));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}
	
		
		
		
	}
	
	@Test
	public void testSetColumns(){
		
	
		try {
			SelectNode selectNodeTest = (SelectNode)(initSelectNode.invoke(null, olapConnection,inputTest));
			
			String columnsBefore = selectNodeTest.getAxisList().get(Axis.COLUMNS.axisOrdinal()).getExpression().toString();
			
			setColumns.invoke(null, columnsTest,selectNodeTest);
			
			String columnsAfter = selectNodeTest.getAxisList().get(Axis.COLUMNS.axisOrdinal()).getExpression().toString();
			
			assertFalse("setColumns did not modify the columns of the SelectNode",columnsAfter.equals(columnsBefore));
			//assert("From clause was not correctly set", "[Traffic]", selectNodeTest.getFrom().toString());
			
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		
		
	}
	
	@Test
	public void testSetRowsOrWhere(){
		try{
		SelectNode selectNodeTest = (SelectNode)(initSelectNode.invoke(null, olapConnection,inputTest));
		setColumns.invoke(null, columnsTest,selectNodeTest);
		
		setRowsOrWhere.invoke(null, rowsTest,selectNodeTest,true);
		
		String rowsAfter = selectNodeTest.getAxisList().get(Axis.ROWS.axisOrdinal()).getExpression().toString();
		
		assertNotNull("ON ROWS did not set", rowsAfter);
		
		setRowsOrWhere.invoke(null, whereTest,selectNodeTest,false);
		String whereAfter = selectNodeTest.getFilterAxis().getExpression().toString();
		assertNotNull("WHERE did not set", whereAfter);
		
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testSetRows(){
		try{
		SelectNode selectNodeTest = (SelectNode)(initSelectNode.invoke(null, olapConnection,inputTest));
		setColumns.invoke(null, columnsTest,selectNodeTest);
		
		setRows.invoke(null, rowsTest,selectNodeTest);
		String rowsAfter = selectNodeTest.getAxisList().get(Axis.ROWS.axisOrdinal()).getExpression().toString();
		assertNotNull("ON ROWS did not set", rowsAfter);
		
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testSetWhere(){
		try{
		SelectNode selectNodeTest = (SelectNode)(initSelectNode.invoke(null, olapConnection,inputTest));
		setColumns.invoke(null, columnsTest,selectNodeTest);
		setRows.invoke(null, rowsTest,selectNodeTest);
		
		setWhere.invoke(null, whereTest,selectNodeTest);
		String whereAfter = selectNodeTest.getFilterAxis().getExpression().toString();
		assertNotNull("WHERE did not set", whereAfter);
		
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testCreateSelectNode(){
		
		try {
			
			SelectNode selectNodeTest = MDXBuilder.createSelectNode(olapConnection, inputTest);
		
			assertTrue("createSelectNode did not return an object of type SelectNode",selectNodeTest instanceof SelectNode);
		
			String rowsAfter = selectNodeTest.getAxisList().get(Axis.ROWS.axisOrdinal()).getExpression().toString();
			assertNotNull("ON ROWS did not set", rowsAfter);
			String whereAfter = selectNodeTest.getFilterAxis().getExpression().toString();
			assertNotNull("WHERE did not set", whereAfter);
			
			
		} catch (Solap4pyException e) {
			e.printStackTrace();
		}
		
	}
	

}
