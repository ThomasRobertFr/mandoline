package fr.solap4py.core;

import java.sql.SQLException;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
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

    public Metadata(OlapConnection connection) {
        try {
            this.catalog = connection.getOlapCatalog();
        } catch (OlapException e) {
            e.printStackTrace();
        }
    }

    public JSONObject query(JSONObject query) throws OlapException, JSONException {

        JSONObject result = new JSONObject();
        JSONObject data = query.getJSONObject("data");
        JSONArray root = data.getJSONArray("root");
        boolean property = data.getBoolean("property");
        JSONArray metadata = null;
        
        try {
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
        	metadata = this.getMembers(root);
                break;
            default:
        	
            }
            
            if (property == true) {
        	
            }
        } catch (OlapException e) {
            
        } catch (Exception e) {
            
        }

        result.put("error", "OK");
        result.put("data", metadata);
        return result;
    }

    private JSONArray getSchemas() throws OlapException, JSONException {
        List<Schema> schemas = this.catalog.getSchemas();
        JSONArray array = new JSONArray();
        for (Schema schema : schemas) {
            JSONObject s = new JSONObject();
            s.put("name", schema.getName());
            array.put(s);
        }

        return array;
    }

    private JSONArray getCubes(JSONArray from) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        JSONArray array = new JSONArray();

        for (Cube cube : cubes) {
            JSONObject s = new JSONObject();
            s.put("id", cube.getName());
            s.put("caption", cube.getCaption());
            array.put(s);
        }

        return array;
    }

    private JSONArray getDimensions(JSONArray from) throws OlapException, JSONException {
        List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
        Cube cube  = null;
        for (Cube c : cubes) {
            if (c.getUniqueName().equals(from.getString(1))) {
                cube = c;
                break;
            }
        }
                
        List<Dimension> dimensions = cube.getDimensions();
        JSONArray array = new JSONArray();

        for (Dimension dimension : dimensions) {
            JSONObject s = new JSONObject();
            s.put("id", dimension.getUniqueName());
            s.put("caption", dimension.getCaption());
            s.put("type", dimension.getDimensionType().toString());
            array.put(s);
        }

        return array;
    }

    private JSONArray getHierarchies(JSONArray from) throws OlapException, JSONException {
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
        JSONArray array = new JSONArray();

        for (Hierarchy hierarchy : hierarchies) {
            JSONObject s = new JSONObject();
            s.put("id", hierarchy.getName());
            s.put("caption", hierarchy.getCaption());
            array.put(s);
        }

        return array;
    }

    private JSONArray getLevels(JSONArray from) throws OlapException, JSONException {
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
        JSONArray array = new JSONArray();

        for (Level level : levels) {
            JSONObject s = new JSONObject();
            s.put("id", level.getName());
            s.put("caption", level.getCaption());
            array.put(s);
        }

        return array;
    }

    private JSONArray getMembers(JSONArray from) throws OlapException, JSONException {
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
        JSONArray array = new JSONArray();

        for (Member member : members) {
            JSONObject s = new JSONObject();
            s.put("id", member.getUniqueName());
            s.put("caption", member.getCaption());
            array.put(s);
        }

        return array;
    }

    private JSONArray getProperties(JSONArray from) throws OlapException, JSONException {
        List<Property> properties = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions()
                                                .get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels()
                                                .get(from.getString(4)).getProperties();
        JSONArray array = new JSONArray();

        for (Property property : properties) {
            JSONObject s = new JSONObject();
            s.put("id", property.getUniqueName());
            s.put("caption", property.getCaption());
            array.put(s);
        }

        return array;
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, JSONException {

        String param = "{ \"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name1]\"], \"property\" : true }}";
        
        Solap4py p = Solap4py.getSolap4Object();
        JSONObject query = new JSONObject(param);
        Metadata m = new Metadata(p.getOlapConnection());

        System.out.println((m.query(query)).toString());
    }
}
