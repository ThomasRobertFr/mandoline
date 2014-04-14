package fr.solap4py.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Property;
import org.olap4j.metadata.Schema;


public class Metadata {

    private Catalog catalog;
    private OlapConnection olapConnection;
    private static final ArrayList<String> USELESS_PROPERTIES = new ArrayList<String>(
	    Arrays.asList("CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
		    "DIMENSION_UNIQUE_NAME", "HIERARCHY_UNIQUE_NAME",
		    "LEVEL_UNIQUE_NAME", "LEVEL_NUMBER", "MEMBER_ORDINAL",
		    "MEMBER_NAME", "MEMBER_UNIQUE_NAME", "MEMBER_TYPE",
		    "MEMBER_GUID", "MEMBER_CAPTION", "CHILDREN_CARDINALITY",
		    "PARENT_LEVEL", "PARENT_UNIQUE_NAME", "PARENT_COUNT",
		    "DESCRIPTION", "$visible", "MEMBER_KEY",
		    "IS_PLACEHOLDERMEMBER", "IS_DATAMEMBER", "DEPTH",
		    "DISPLAY_INFO", "VALUE"));

    public Metadata(OlapConnection connection) {
	try {
	    this.catalog = connection.getOlapCatalog();
	    this.olapConnection = connection;
	} catch (OlapException e) {
	    e.printStackTrace();
	}
    }

    public JSONObject query(JSONObject query, JSONObject jsonResult) throws OlapException,
		JSONException, Solap4pyException {
	JSONObject data = null;
	JSONArray root = null;
	boolean withProperties;

	try {
	    data = query.getJSONObject("data");
	} catch (JSONException e) {
	    return (new Solap4pyException(ErrorType.BAD_REQUEST,
		    "'data' field not specified or invalid").getJSON());
	}
	try {
	    root = data.getJSONArray("root");
	} catch (JSONException e) {
	    return (new Solap4pyException(ErrorType.BAD_REQUEST,
		    "'root' field not specified or invalid").getJSON());
	}

	switch (root.length()) {
	case 0:
	    jsonResult.put("data", this.getSchemas());
	    break;
	case 1:
	    jsonResult.put("data", this.getCubes(root));
	    break;
	case 2:
	    jsonResult.put("data", this.getDimensions(root));
	    break;
	case 3:
	    jsonResult.put("data", this.getHierarchies(root));
	    break;
	case 4:
	    try {
		withProperties = data.getBoolean("withProperties");
	    } catch (JSONException e) {
		return (new Solap4pyException(ErrorType.BAD_REQUEST,
			"'withProperties' field not specified or invalid")
			.getJSON());
	    }
	    jsonResult.put("data", this.getLevels(root, withProperties));
	    break;
	case 5:
	    try {
		withProperties = data.getBoolean("withProperties");
	    } catch (JSONException e) {
		return (new Solap4pyException(ErrorType.BAD_REQUEST,
			"'withProperties' field not specified or invalid")
			.getJSON());
	    }
	    jsonResult.put("data", this.getMembers(root, withProperties, 0));
	    break;
	case 6:
	    try {
		withProperties = data.getBoolean("withProperties");
	    } catch (JSONException e) {
		return (new Solap4pyException(ErrorType.BAD_REQUEST,
			"'withProperties' field not specified or invalid")
			.getJSON());
	    }
	    int granularity;
	    try {
		granularity = data.getInt("granularity");
	    } catch (JSONException e) {
		return (new Solap4pyException(ErrorType.BAD_REQUEST,
			"'granularity' field not specified or invalid")
			.getJSON());
	    }
	    if (granularity < 0) {
		return (new Solap4pyException(ErrorType.BAD_REQUEST,
			"'granulaity' must be a positive integer'").getJSON());
	    }
	    jsonResult.put("data", this.getMembers(root, withProperties, granularity));
	    break;
	default:
	    return (new Solap4pyException(ErrorType.BAD_REQUEST,
		    "Too many parameters in array 'root'").getJSON());
	}

	return jsonResult;
    }

    private JSONObject getSchemas() throws Solap4pyException {
	List<Schema> schemas = null;
	JSONObject result = new JSONObject();
	try {
	    schemas = this.catalog.getSchemas();

	    for (Schema schema : schemas) {
		JSONObject s = new JSONObject();
		s.put("caption", schema.getName());
		result.put(schema.getName(), s);
	    }
	} catch (OlapException e) {
	    throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
		    "An error occured while trying to retrieve schemas");
	} catch (JSONException e) {

	}

	return result;
    }

    private JSONObject getCubes(JSONArray from) throws OlapException,
	    JSONException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0))
		.getCubes();
	JSONObject result = new JSONObject();
	for (Cube cube : cubes) {
	    JSONObject s = new JSONObject();
	    s.put("caption", cube.getCaption());
	    result.put(cube.getUniqueName(), s);
	}

	return result;
    }

    private JSONObject getDimensions(JSONArray from) throws OlapException,
	    JSONException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0))
		.getCubes();
	Cube cube = null;
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
	    switch (dimension.getDimensionType().toString()) {
	    case "TIME":
		s.put("type", "Time");
		s.put("caption", dimension.getCaption());
		result.put(dimension.getUniqueName(), s);
		break;
	    case "MEASURE":
		break;
	    case "OTHER":
		Iterator<Property> i = dimension.getHierarchies().get(1)
			.getLevels().get(1).getMembers().get(0).getProperties()
			.iterator();
		Boolean hasGeometry = false;
		String type;
		while (i.hasNext() & !hasGeometry) {
		    hasGeometry = i.next().getCaption().substring(0, 4)
			    .equals("Geom");
		}
		if (hasGeometry)
		    type = "Geometry";
		else
		    type = "Standard";
		s.put("type", type);
		s.put("caption", dimension.getCaption());
		result.put(dimension.getUniqueName(), s);
		break;
	    default:
		s.put("type", "Standard");
		s.put("caption", dimension.getCaption());
		result.put(dimension.getUniqueName(), s);
	    }
	}

	for (Measure m : cube.getMeasures()) {
	    JSONObject s = new JSONObject();
	    s.put("type", "Measure");
	    s.put("caption", m.getCaption());
	    s.put("aggregator", m.getAggregator().toString());
	    result.put(m.getUniqueName(), s);
	}

	return result;
    }

    private JSONObject getHierarchies(JSONArray from) throws OlapException,
	    JSONException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0))
		.getCubes();
	Cube cube = null;
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

    private JSONArray getLevels(JSONArray from, boolean withProperties)
	    throws OlapException, JSONException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0))
		.getCubes();
	Cube cube = null;
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
	Hierarchy hierarchy = null;
	for (Hierarchy h : hierarchies) {
	    if (h.getUniqueName().equals(from.getString(3))) {
		hierarchy = h;
		break;
	    }
	}
	List<Level> levels = hierarchy.getLevels();
	JSONArray result = new JSONArray();
	for (Level level : levels) {
	    JSONObject s = new JSONObject();
	    s.put("caption", level.getCaption());
	    s.put("id", level.getUniqueName());
	    if (withProperties == true) {
		s.put("list-properties", this.getLevelProperties(level));
	    }
	    result.put(level.getDepth(), s);
	}

	return result;
    }

    private JSONObject getMembers(JSONArray from, boolean withProperties,
	    int granularity) throws OlapException, JSONException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0))
		.getCubes();
	Cube cube = null;
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
	Hierarchy hierarchy = null;
	for (Hierarchy h : hierarchies) {
	    if (h.getUniqueName().equals(from.getString(3))) {
		hierarchy = h;
		break;
	    }
	}
	List<Level> levels = hierarchy.getLevels();
	Level level = null;
	for (Level l : levels) {
	    if (l.getUniqueName().equals(from.getString(4))) {
		level = l;
		break;
	    }
	}
	String current = null;
	List<Member> tmp = level.getMembers();
	List<Member> members = new LinkedList<Member>();
	for (Member m : tmp) {
	    if (m.getUniqueName().equals(current) == false) {
		members.add(m);
		current = m.getUniqueName();
	    }
	}

	JSONObject result = new JSONObject();

	if (from.length() == 6) {
	    for (Member member : members) {
		if (member.getUniqueName().equals(from.getString(5))) {
		    LinkedList<Member> memberArray = new LinkedList<Member>(
			    Arrays.asList(member));
		    for (int i = 0; i < granularity; i++) {
			LinkedList<Member> list = new LinkedList<Member>();
			for (Member m : memberArray) {
			    list.addAll(m.getChildMembers());
			}
			memberArray = list;
		    }
		    members = memberArray;
		    break;
		}
	    }
	}
	for (Member member : members) {
	    JSONObject s = new JSONObject();
	    s.put("caption", member.getCaption());
	    if (from.getString(2).equals("[Measures]")) {
		s.put("description", member.getDescription());
	    }
	    if (withProperties == true) {
		this.getMemberProperties(from, member, s);
	    }
	    result.put(member.getUniqueName(), s);
	}

	return result;
    }

    private JSONObject getLevelProperties(Level level) throws OlapException,
	    JSONException {
	List<Property> properties = level.getProperties();
	JSONObject result = new JSONObject();
	for (Property property : properties) {
	    JSONObject s = new JSONObject();
	    s.put("caption", property.getCaption());
	    if (Metadata.USELESS_PROPERTIES.contains(property.getUniqueName()) == false) {
		if (property.getCaption().substring(0, 4).equals("Geom")) {
		    s.put("type", "Geometry");
		} else {
		    s.put("type", "Standard");
		}
		result.put(property.getUniqueName(), s);
	    }
	}

	return result;
    }

    private void getMemberProperties(JSONArray from, Member member,
	    JSONObject result) throws OlapException, JSONException {
	for (Property property : member.getProperties()) {
	    if (Metadata.USELESS_PROPERTIES.contains(property.getUniqueName()) == false) {
		if (property.getCaption().substring(0, 4).equals("Geom")) {
		    result.put(
			    property.getUniqueName(),
			    this.getGeometry(from, member,
				    property.getCaption()));
		} else {
		    result.put(property.getUniqueName(),
			    member.getPropertyFormattedValue(property));
		}
	    }
	}
    }

    private String getGeometry(JSONArray from, Member member,
	    String geometricProperty) throws OlapException, JSONException {
	OlapStatement statement = this.olapConnection.createStatement();
	String nameMember = member.getUniqueName();
	CellSet cellSet = statement
		.executeOlapQuery("with member [Measures].[geo] as "
			+ nameMember + ".Properties(\"" + geometricProperty
			+ "\") select [Measures].[geo] ON COLUMNS from "
			+ from.getString(1));

	return cellSet.getCell(0).getFormattedValue();
    }

    public static void main(String[] args) throws ClassNotFoundException,
	    SQLException, JSONException {

	String param = "{ \"queryType\" : \"metadata\","
		+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\"], \"withProperties\" : false, \"granularity\" : 1}}";

	Solap4py p = Solap4py.getSolap4Object();
	JSONObject query = new JSONObject(param);
	Metadata m = new Metadata(p.getOlapConnection());
	JSONObject result = new JSONObject();
	try {
	    result = m.query(query, result);
	     //System.out.println(result.getJSONObject("data").getJSONObject("[Zone.Name].[All Zone.Names].[France]"));
	    System.out.println(result);
	} catch (Solap4pyException e) {
	    System.out.println(e.getJSON());
	}
    }
}
