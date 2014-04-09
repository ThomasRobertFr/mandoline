package fr.solap4py.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Property;
import org.olap4j.metadata.Schema;

public class Metadata {

    private Catalog catalog;
    private OlapConnection olapConnection;
    
    public Metadata(OlapConnection connection) {
        try {
            this.catalog = connection.getOlapCatalog();
            this.olapConnection = connection;
        } catch (OlapException e) {
            e.printStackTrace();
        }
    }

    public JSONObject query(JSONObject query) throws OlapException, JSONException, Solap4pyException {
        JSONObject result = new JSONObject();
        JSONObject data = null;
        JSONArray root = null;
        JSONObject metadata = null;
	boolean withProperties;
	int granularity = 0;
        
        try {
             data = query.getJSONObject("data");
        } catch (JSONException e) {
            return (new Solap4pyException(ErrorType.BAD_REQUEST, "'data' field not specified").getJSON());
        }
        try {
            root = data.getJSONArray("root");
        } catch(JSONException e) {
            return (new Solap4pyException(ErrorType.BAD_REQUEST, "'root' field not specified").getJSON());
        }
        
        switch (root.length()) {
            case 0 :
        	metadata = this.getSchemas();
                break;
            case 1 :
        	metadata = this.getCubes(root);
                break;
            case 2 :
        	metadata = this.getDimensions(root);
                break;
            case 3 :
        	metadata = this.getHierarchies(root);
                break;
            case 4 :
        	metadata = this.getLevels(root);
                break;
            case 5 :
        	try {
        	    withProperties = data.getBoolean("withProperties");
        	} catch (JSONException e) {
        	    return (new Solap4pyException(ErrorType.BAD_REQUEST, "'withProperties' field not specified").getJSON());
        	}
        	metadata = this.getMembers(root, withProperties, granularity);
                break;
            case 6 :
        	try {
        	    withProperties = data.getBoolean("withProperties");
        	} catch (JSONException e) {
        	    return (new Solap4pyException(ErrorType.BAD_REQUEST, "'withProperties' field not specified").getJSON());
        	}
        	try {
        	    granularity = data.getInt("granularity");
        	} catch (JSONException e) {
        	    return (new Solap4pyException(ErrorType.BAD_REQUEST, "'granularity' field not specified").getJSON());
        	}
        	break;
            default :
        	return (new Solap4pyException(ErrorType.BAD_REQUEST, "too many parameters in array 'root'").getJSON());
        }
            

        result.put("error", "OK");
        result.put("data", metadata);
        return result;
    }

    private JSONObject getSchemas() throws OlapException, JSONException {
        List<Schema> schemas = this.catalog.getSchemas();
        JSONObject result = new JSONObject();
        for (Schema schema : schemas) {
            JSONObject s = new JSONObject();
            s.put("caption", schema.getName());
            result.put(schema.getName(), s);
        }

        return result;
    }

    private JSONObject getCubes(JSONArray from) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        JSONObject result = new JSONObject();
        for (Cube cube : cubes) {
            JSONObject s = new JSONObject();
            s.put("caption", cube.getCaption());
            result.put(cube.getUniqueName(), s);
        }

        return result;
    }

    private JSONObject getDimensions(JSONArray from) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        Cube cube  = null;
        for (Cube c : cubes) {
            if (c.getUniqueName().equals(from.getString(1))) {
                cube = c;
                break;
            }
        }
                
        List<Dimension> dimensions = cube.getDimensions();
        JSONObject result = new JSONObject();
        for (Dimension dimension : dimensions) {
            JSONObject s = new JSONObject();
            s.put("caption", dimension.getCaption());
            s.put("type", dimension.getDimensionType().toString());
            result.put(dimension.getUniqueName(), s);
        }

        return result;
    }

    private JSONObject getHierarchies(JSONArray from) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        Cube cube  = null;
        for (Cube c : cubes) {
            if (c.getUniqueName().equals(from.getString(1))) {
                cube = c;
                break;
            }
        }
        List<Dimension> dimensions = cube.getDimensions();
        Dimension dimension = null;
        for (Dimension d : dimensions) {
            if (d.getUniqueName().equals(from.getString(2))) {
                dimension = d;
                break;
            }
        }
        List<Hierarchy> hierarchies = dimension.getHierarchies();
        JSONObject result = new JSONObject();
        for (Hierarchy hierarchy : hierarchies) {
            JSONObject s = new JSONObject();
            s.put("caption", hierarchy.getCaption());
            result.put(hierarchy.getUniqueName(), s);
        }

        return result;
    }

    private JSONObject getLevels(JSONArray from) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        Cube cube  = null;
        for (Cube c : cubes) {
            if (c.getUniqueName().equals(from.getString(1))) {
                cube = c;
                break;
            }
        }
        List<Dimension> dimensions = cube.getDimensions();
        Dimension dimension = null;
        for (Dimension d : dimensions) {
            if (d.getUniqueName().equals(from.getString(2))) {
                dimension = d;
                break;
            }
        }
        List<Hierarchy> hierarchies= dimension.getHierarchies();
        Hierarchy hierarchy = null;
        for (Hierarchy h : hierarchies) {
            if (h.getUniqueName().equals(from.getString(3))) {
                hierarchy = h;
                break;
            }
        }
        List<Level> levels = hierarchy.getLevels();
        JSONObject result = new JSONObject();
        for (Level level : levels) {
            JSONObject s = new JSONObject();
            s.put("caption", level.getCaption());
            result.put(level.getUniqueName(), s);
        }

        return result;
    }

    private JSONObject getMembers(JSONArray from, boolean withProperties, int granulatity) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        Cube cube  = null;
        for (Cube c : cubes) {
            if (c.getUniqueName().equals(from.getString(1))) {
                cube = c;
                break;
            }
        }
        List<Dimension> dimensions = cube.getDimensions();
        Dimension dimension = null;
        for (Dimension d : dimensions) {
            if (d.getUniqueName().equals(from.getString(2))) {
                dimension = d;
                break;
            }
        }
        List<Hierarchy> hierarchies= dimension.getHierarchies();
        Hierarchy hierarchy = null;
        for (Hierarchy h : hierarchies) {
            if (h.getUniqueName().equals(from.getString(3))) {
                hierarchy = h;
                break;
            }
        }
        List<Level> levels = hierarchy.getLevels();
        Level level  = null;
        for (Level l : levels) {
            if (l.getUniqueName().equals(from.getString(4))) {
                level = l;
                break;
            }
        }
        List<Member> members = level.getMembers();
        JSONObject result = new JSONObject();

        for (Member member : members) {
            JSONObject s = new JSONObject();
            //s.put("id", member.getUniqueName());
            s.put("caption", member.getCaption());
            result.put(member.getUniqueName(), s);
        }

        return result;
    }

    private JSONObject getLevelProperties(JSONArray from) throws OlapException, JSONException {
        List<Property> properties = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions()
                                                .get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels()
                                                .get(from.getString(4)).getProperties();
        JSONObject result = new JSONObject();
        for (Property property : properties) {
            JSONObject s = new JSONObject();
            s.put("caption",property.getCaption());
            if(property.getCaption().substring(0,5).equals("Geom")) 
            	s.put("type","Geometry");
            else
            	s.put("type","Standard");
            result.put(property.getUniqueName(),s);
        }

        return result;
    }
    
    private JSONObject getMemberProperties(JSONArray from, Member member) throws OlapException, JSONException {
        
        JSONObject result = new JSONObject();
        result.put("caption",member.getCaption());
        for (Property property : member.getProperties()) {
            JSONObject s = new JSONObject();
            if(property.getCaption().substring(0,5).equals("Geom")) 
            	s.put(property.getUniqueName(),getGeometry(from,member,property.getCaption()));
            else
            	s.put(property.getUniqueName(),member.getPropertyFormattedValue(property));
            result.put(property.getUniqueName(), s);
        }

        return result;
    }
    
    private String getGeometry(JSONArray from, Member member, String geometricProperty) throws OlapException, JSONException{
        OlapStatement statement = this.olapConnection.createStatement();
        
        String nameCube = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getUniqueName();
        String nameMember = member.getUniqueName();

        CellSet cellSet = statement.executeOlapQuery("with member [Measures].[geo] as '" + nameMember + "].Properties(\" " + geometricProperty + " \")' select [Measures].[geo] ON COLUMNS from [" + 
		nameCube + "]" );
        
        return cellSet.getCell(0).getFormattedValue();	
    	
    	
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, JSONException {

       /* String param = "{ \"queryType\" : \"metadata\"," +
        		"\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name1]\"], \"withProperties\" : true, \"granularity\" : 0}}";
        
        Solap4py p = Solap4py.getSolap4Object();
        JSONObject query = new JSONObject(param);
        Metadata m = new Metadata(p.getOlapConnection());
        
        try {
            System.out.println((m.query(query)).toString());
        } catch (Solap4pyException e) {
            System.out.println(e.getJSON());
        }*/
    	Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
        Connection connection = DriverManager.getConnection("jdbc:xmla:Server=http://postgres:westcoast@192.168.1.1:8080/geomondrian/xmla");
        Metadata meta = new Metadata(connection.unwrap(OlapConnection.class));
        Catalog catalog = connection.unwrap(OlapConnection.class).getOlapCatalog();
        System.out.println(catalog.getSchemas().get("Traffic").getCubes().get("Traffic").getDimensions().get("Zone").getHierarchies().get("Zone.Name").getLevels().get("Name0").getMembers().get(0).getUniqueName());
        System.out.println("Done.");
    	
    }
}
