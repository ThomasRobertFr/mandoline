package fr.solap4py.core;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.olap4j.OlapConnection;
import org.olap4j.mdx.SelectNode;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;

public class MetadataTest {

    private Solap4py solap4py;
    private OlapConnection olapConnection;
    private JSONObject inputTest;
    private Metadata metadata;
    
    private Method query;
    private Method getSchemas;
    private Method getCubes;
    private Method getDimensions;
    private Method getHierarchies;
    private Method getLevels;
    private Method getMembers;
    private Method getLevelProperties;
    private Method getMemberProperties;    
    private Method getGeometry;   
    
    @Before
    public void setUp() throws Exception {
        
        solap4py = Solap4py.getSolap4Object();
        olapConnection = solap4py.getOlapConnection();
        
        metadata = new Metadata(olapConnection);

        
        query = Metadata.class.getDeclaredMethod("query", JSONObject.class, JSONObject.class);
        query.setAccessible(true);
        
        getSchemas = Metadata.class.getDeclaredMethod("getSchemas");
        getSchemas.setAccessible(true); 
 
        getCubes = Metadata.class.getDeclaredMethod("getCubes", JSONArray.class);
        getCubes.setAccessible(true); 
        
        getDimensions = Metadata.class.getDeclaredMethod("getDimensions", JSONArray.class);
        getDimensions.setAccessible(true); 
        
        getHierarchies = Metadata.class.getDeclaredMethod("getHierarchies", JSONArray.class);
        getHierarchies.setAccessible(true); 
        
        getLevels = Metadata.class.getDeclaredMethod("getLevels", JSONArray.class, boolean.class);
        getLevels.setAccessible(true); 
        
        getMembers = Metadata.class.getDeclaredMethod("getMembers", JSONArray.class, boolean.class, int.class);
        getMembers.setAccessible(true); 
        
        getLevelProperties = Metadata.class.getDeclaredMethod("getLevelProperties", Level.class);
        getLevelProperties.setAccessible(true); 
        
        getMemberProperties = Metadata.class.getDeclaredMethod("getMemberProperties", JSONArray.class, Member.class, JSONObject.class );
        getMemberProperties.setAccessible(true); 
        
        getGeometry = Metadata.class.getDeclaredMethod("getGeometry", JSONArray.class, Member.class, String.class);
        getGeometry.setAccessible(true); 
    }

    @After
    public void tearDown() throws Exception {
        olapConnection.close();
    }

    @Test
    public void testGetLevels() {
        
        String param = "{ \"queryType\" : \"metadata\","
        		+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\" : false}}";
        try {
        	JSONObject query = new JSONObject(param);
        	JSONObject data = query.getJSONObject("data");
        	JSONArray root = data.getJSONArray("root");
        	
        	JSONArray result = new JSONArray();
        	result = (JSONArray)(getLevels.invoke(metadata, root,false));
        	
        	
        	assertTrue("the result does not contain the first level", result.getJSONObject(0).get("id").equals("[Zone.Name].[(All)]"));
        	assertTrue("the result does not contain the second level", result.getJSONObject(1).get("id").equals("[Zone.Name].[Name0]"));
        	assertTrue("the result does not contain the third level", result.getJSONObject(2).get("id").equals("[Zone.Name].[Name1]"));
        	assertTrue("the result does not contain the fourth level", result.getJSONObject(3).get("id").equals("[Zone.Name].[Name2]"));
        	assertTrue("the result does not contain the fifth level", result.getJSONObject(4).get("id").equals("[Zone.Name].[Name3]"));
        	
        	
        	result = (JSONArray)(getLevels.invoke(metadata, root,true));
        	
        	assertTrue("the first level does not retrieve his properties", result.getJSONObject(0).has("list-properties"));
        	assertTrue("the second level does not retrieve his properties", result.getJSONObject(1).has("list-properties"));
        	assertTrue("the third level does not retrieve his properties", result.getJSONObject(2).has("list-properties"));
        	assertTrue("the fourth level does not retrieve his properties", result.getJSONObject(3).has("list-properties"));
        	assertTrue("the fifth level does not retrieve his properties", result.getJSONObject(4).has("list-properties"));
        	
              	  
        	} catch (JSONException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
            
        
        
    }
    
    @Test
    public void testgetMembers() {
        
        String param = "{ \"queryType\" : \"metadata\","
        		+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\" : false, \"granularity\" : 1}}";
        try {
        	JSONObject query = new JSONObject(param);
        	JSONObject data = query.getJSONObject("data");
        	JSONArray root = data.getJSONArray("root");
        	
        	JSONObject result = new JSONObject();
        	result = (JSONObject)(getMembers.invoke(metadata, root,false,1));
        	
        	
        	assertTrue("the result does not contain the first member",result.has("[Zone.Name].[All Zone.Names].[Croatia]"));
        	assertTrue("the result does not contain the last member",result.has("[Zone.Name].[All Zone.Names].[Spain]"));
        	
        	result = (JSONObject)(getMembers.invoke(metadata, root,true,1));
        	assertTrue("the result does not contain the first member's properties",result.getJSONObject("[Zone.Name].[All Zone.Names].[Croatia]").has("Traffic Cube - Zone.Name Hierarchy - Name0 Level - Geom Property"));
        	assertTrue(" the result does not contain the last member's properties  ", result.getJSONObject("[Zone.Name].[All Zone.Names].[Spain]").has("Traffic Cube - Zone.Name Hierarchy - Name0 Level - Geom Property"));
        	        	
        	
        	} catch (JSONException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
            
        
        
    }
    
    @Test
    public void testgetHierarchies() {
        
        String param = "{ \"queryType\" : \"metadata\","
        		+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\"]}}";
        
       // "{ \"queryType\" : \"metadata\","
		//+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"]}}";
        
        
        
        try {
        	JSONObject query = new JSONObject(param);
        	JSONObject data = query.getJSONObject("data");
        	JSONArray root = data.getJSONArray("root");
        	
        	JSONObject result = (JSONObject)(getHierarchies.invoke(metadata, root));
        	
        	
        	assertTrue("the result does not contain the first hierarchy name",result.has("[Zone.Name]"));
        	assertTrue("the result does not contain the second hierarchy name",result.has("[Zone.Reference]"));
        	
        	        	
        	} catch (JSONException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
            
        
        
    }
    
    @Test
    public void testGetDimensions() {
        
        String param = "{ \"queryType\" : \"metadata\","
        		+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\"]}}";
        
       // "{ \"queryType\" : \"metadata\","
		//+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"]}}";
        
        
        
        try {
        	JSONObject query = new JSONObject(param);
        	JSONObject data = query.getJSONObject("data");
        	JSONArray root = data.getJSONArray("root");
        	
        	JSONObject result = (JSONObject)(getDimensions.invoke(metadata, root));
        	
        	
        	assertTrue("the result does not contain the first  Dimension name",result.has("[Zone]"));
        	assertTrue("the result does not contain the second Dimension name",result.has("[Measures].[Goods Quantity]"));
        	assertTrue("the result does not contain the second Dimension name",result.has("[Measures].[Max Quantity]"));
        	assertTrue("the result does not contain the second Dimension name",result.has("[Time]"));
        	
        	
        	        	        	
        	} catch (JSONException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
            
        
        
    }
    
    @Test
    public void testGetCubes() {
        
        String param = "{ \"queryType\" : \"metadata\","
        		+ "\"data\" : { \"root\" : [\"Traffic\"]}}";
        
       // "{ \"queryType\" : \"metadata\","
		//+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"]}}";
        
        
        
        try {
        	JSONObject query = new JSONObject(param);
        	JSONObject data = query.getJSONObject("data");
        	JSONArray root = data.getJSONArray("root");
        	
        	JSONObject result = (JSONObject)(getCubes.invoke(metadata, root));
        	
        	
        	assertTrue("the result does not contain the first cube name",result.has("[Traffic]"));
        	
        	
        	
        	System.out.println(result);
        	        	
        	} catch (JSONException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
            
        
        
    }
    
    

}
