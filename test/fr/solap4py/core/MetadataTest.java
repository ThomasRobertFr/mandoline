package fr.solap4py.core;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.json.JSONArray;
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
    public void test() {
        fail("Not yet implemented");
    }

}
