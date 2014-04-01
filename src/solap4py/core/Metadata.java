package solap4py.core;


import java.lang.reflect.Method;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;

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

    public JsonObject query(JsonObject query) throws OlapException {
	
	JsonObjectBuilder result = Json.createObjectBuilder();
	result.add("error", "OK");
	JsonArray data = null;
	JsonArray from = query.getJsonArray("from");
	
	switch(query.getString("get")) {
	case "schema" :
	    data = this.getSchemas();
	    break;
	case "cube" :
	    data = this.getCubes(from);
	    break;
	case "dimension" :
	    data = this.getDimensions(from);
	    break;
	case "measure" :
	    data = this.getMeasures(from);
	    break;
	case "hierarchy" :
	    data = this.getHierarchies(from);
	    break;
	case "level" :
	    data = this.getLevels(from);
	    break;
	case "member" :
	    data = this.getMembers(from);
	    break;
	case "property" :
	    data = this.getProperties(from);
	    break;
	    default :
		
	}
	
	result.add("data", data);
	return result.build();
    }

    
    private JsonArray getSchemas() throws OlapException {
	List<Schema> schemas = this.catalog.getSchemas();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Schema schema : schemas) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("name", schema.getName());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
    
    private JsonArray getCubes(JsonArray from) throws OlapException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Cube cube : cubes) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", cube.getName());
	    s.add("caption", cube.getCaption());
	    builder.add(s.build());
	}
	
	return builder.build();
    }

    private JsonArray getDimensions(JsonArray from) throws OlapException {
	List<Dimension> dimensions = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Dimension dimension : dimensions) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", dimension.getUniqueName());
	    s.add("caption", dimension.getCaption());
	    s.add("type", dimension.getDimensionType().toString());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
   
    private JsonArray getMeasures(JsonArray from) throws OlapException {
	List<Measure> measures = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getMeasures();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Measure measure : measures) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", measure.getName());
	    s.add("caption", measure.getCaption());
	    s.add("aggregator", measure.getAggregator().toString());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
    
    private JsonArray getHierarchies(JsonArray from) throws OlapException {
	List<Hierarchy> hierarchies = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions().get(from.getString(2)).getHierarchies();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Hierarchy hierarchy : hierarchies) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", hierarchy.getName());
	    s.add("caption", hierarchy.getCaption());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
    
    private JsonArray getLevels(JsonArray from) throws OlapException {
	List<Level> levels = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions().get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Level level : levels) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", level.getName());
	    s.add("caption", level.getCaption());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
    
    private JsonArray getMembers(JsonArray from) throws OlapException {
	List<Member> members = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions().get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels().get(from.getString(4)).getMembers();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Member member : members) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", member.getUniqueName());
	    s.add("caption", member.getCaption());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
    
    private JsonArray getProperties(JsonArray from) throws OlapException {
	List<Property> properties = this.catalog.getSchemas().get(from.getString(0)).getCubes().get(from.getString(1)).getDimensions().get(from.getString(2)).getHierarchies().get(from.getString(3)).getLevels().get(from.getString(4)).getProperties();
	JsonArrayBuilder builder = Json.createArrayBuilder();
	
	for (Property property : properties) {
	    JsonObjectBuilder s = Json.createObjectBuilder();
	    s.add("id", property.getUniqueName());
	    s.add("caption", property.getCaption());
	    builder.add(s.build());
	}
	
	return builder.build();
    }
}
