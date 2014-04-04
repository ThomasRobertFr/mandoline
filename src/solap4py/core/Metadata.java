package solap4py.core;

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
import org.olap4j.metadata.Measure;
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
        result.put("error", "OK");
        JSONArray data = null;
        JSONArray from = query.getJSONArray("from");

        switch (query.getString("get")) {
        case "schema":
            data = this.getSchemas();
            break;
        case "cube":
            data = this.getCubes(from);
            break;
        case "dimension":
            data = this.getDimensions(from);
            break;
        case "measure":
            data = this.getMeasures(from);
            break;
        case "hierarchy":
            data = this.getHierarchies(from);
            break;
        case "level":
            data = this.getLevels(from);
            break;
        case "member":
            data = this.getMembers(from);
            break;
        case "property":
            data = this.getProperties(from);
            break;
        default:

        }

        result.put("data", data);
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
        List<Dimension> dimensions = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions();
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

    private JSONArray getMeasures(JSONArray from) throws OlapException, JSONException {
        List<Measure> measures = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getMeasures();
        JSONArray array = new JSONArray();

        for (Measure measure : measures) {
            JSONObject s = new JSONObject();
            s.put("id", measure.getName());
            s.put("caption", measure.getCaption());
            s.put("aggregator", measure.getAggregator().toString());
            array.put(s);
        }

        return array;
    }

    private JSONArray getHierarchies(JSONArray from) throws OlapException, JSONException {
        List<Hierarchy> hierarchies = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions()
                                                  .get(from.getString(2)).getHierarchies();
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
        List<Level> levels = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions()
                                         .get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels();
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
        List<Member> members = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions()
                                           .get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels()
                                           .get(from.getString(4)).getMembers();
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
}
