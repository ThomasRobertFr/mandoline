/**
 * @author Rémy Chevalier
 * @author Alexandre Peltier
 * @version 1.02
 */
package fr.solap4py.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

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
    private static final List<String> USELESS_PROPERTIES = new ArrayList<String>(Arrays.asList("CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
                                                                                               "DIMENSION_UNIQUE_NAME",
                                                                                               "HIERARCHY_UNIQUE_NAME",
                                                                                               "LEVEL_UNIQUE_NAME", "LEVEL_NUMBER",
                                                                                               "MEMBER_ORDINAL", "MEMBER_NAME",
                                                                                               "MEMBER_UNIQUE_NAME", "MEMBER_TYPE",
                                                                                               "MEMBER_GUID", "MEMBER_CAPTION",
                                                                                               "CHILDREN_CARDINALITY", "PARENT_LEVEL",
                                                                                               "PARENT_UNIQUE_NAME", "PARENT_COUNT",
                                                                                               "DESCRIPTION", "$visible", "MEMBER_KEY",
                                                                                               "IS_PLACEHOLDERMEMBER", "IS_DATAMEMBER",
                                                                                               "DEPTH", "DISPLAY_INFO", "VALUE"));
    private static final Logger LOGGER = Logger.getLogger(Metadata.class.getName());

    /**
     * Retrieve the catalog form the connection.
     * @param connection
     */
    public Metadata(OlapConnection connection) {
        try {
            this.catalog = connection.getOlapCatalog();
            this.olapConnection = connection;
        } catch (OlapException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
        }
    }

    /**
     * Returns in a JSONObject the metadata indicated in query.
     * 
     * @param query
     * @param jsonResult
     * @return the response of the query
     * @throws Solap4pyException
     */
    public JSONObject query(JSONObject query, JSONObject jsonResult) throws Solap4pyException {
        JSONArray root = null;
        boolean withProperties;

        try {
            root = query.getJSONArray("root");
        } catch (JSONException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "'root' field not specified or invalid");
        }

        try {
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
                    withProperties = query.getBoolean("withProperties");
                } catch (JSONException e) {
                    LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
                    throw new Solap4pyException(ErrorType.BAD_REQUEST, "'withProperties' field not specified or invalid");
                }
                jsonResult.put("data", this.getLevels(root, withProperties));
                break;
            case 5:
                try {
                    withProperties = query.getBoolean("withProperties");
                } catch (JSONException e) {
                    LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
                    throw new Solap4pyException(ErrorType.BAD_REQUEST, "'withProperties' field not specified or invalid");
                }
                jsonResult.put("data", this.getMembers(root, withProperties, 0));
                break;
            case 6:
                try {
                    withProperties = query.getBoolean("withProperties");
                } catch (JSONException e) {
                    LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
                    throw new Solap4pyException(ErrorType.BAD_REQUEST, "'withProperties' field not specified or invalid");
                }
                int granularity;
                try {
                    granularity = query.getInt("granularity");
                } catch (JSONException e) {
                    LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
                    throw new Solap4pyException(ErrorType.BAD_REQUEST, "'granularity' field not specified or invalid");
                }
                if (granularity < 0) {
                    throw new Solap4pyException(ErrorType.BAD_REQUEST, "'granularity' must be a positive integer'");
                }
                jsonResult.put("data", this.getMembers(root, withProperties, granularity));
                break;
            default:
                throw new Solap4pyException(ErrorType.BAD_REQUEST, "Too many parameters in array 'root'");
            }
        } catch (JSONException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "An error occured while building json result");
        }
        return jsonResult;
    }

    /**
     * Returns all schemas of the database.
     * 
     * @return the schemas existing in the database
     * @throws Solap4pyException
     */
    private JSONObject getSchemas() throws Solap4pyException, JSONException {
        List<Schema> schemas = null;
        JSONObject result = new JSONObject();
        try {
            schemas = this.catalog.getSchemas();

            for (Schema schema : schemas) {
                JSONObject s = new JSONObject();
                s.put("caption", schema.getName());
                result.put(schema.getName(), s);
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "An error occured while trying to retrieve schemas");
        } catch (JSONException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "An error occured while building json result");
        }
        return result;
    }

    /**
     * Returns all cube's names from a schema specified in from. 
     * 
     * @param from
     * @return Names of the cubes existing in a schema.
     * @throws Solap4pyException
     */
    private JSONObject getCubes(JSONArray from) throws Solap4pyException, JSONException {
        JSONObject result = new JSONObject();
        try {
            List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
            for (Cube cube : cubes) {
                JSONObject s = new JSONObject();
                s.put("caption", cube.getCaption());
                result.put(cube.getUniqueName(), s);
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid schema identifier");
        }
        return result;
    }

    /**
     * Returns the all the dimensions's names from a cube specified in from
     * @param from
     * @return the dimensions existing in the cube specified in from
     * @throws Solap4pyException
     */
    private JSONObject getDimensions(JSONArray from) throws Solap4pyException, JSONException {
        JSONObject result = new JSONObject();
        try {
            Cube cube = this.extractCube(from);

            List<Dimension> dimensions = cube.getDimensions();
            for (Dimension dimension : dimensions) {
                JSONObject s = new JSONObject();
                switch (dimension.getDimensionType().toString()) {
                case "TIME":
                    s.put("type", "Time");
                    s.put("caption", dimension.getCaption());
                    result.put(dimension.getUniqueName(), s);
                    break;
                case "MEASURE":
                    s.put("type", "Measure");
                    s.put("caption", dimension.getCaption());
                    result.put(dimension.getUniqueName(), s);

                    break;
                case "OTHER":
                    Iterator<Property> i = dimension.getHierarchies().get(1).getLevels().get(1).getMembers().get(0).getProperties()
                                                    .iterator();
                    Boolean hasGeometry = false;
                    String type;
                    while (i.hasNext() & !hasGeometry) {
                        hasGeometry = "Geom".equals(i.next().getCaption().substring(0, 4));
                    }
                    if (hasGeometry) {
                        type = "Geometry";
                    } else {
                        type = "Standard";
                    }
                    s.put("type", type);
                    s.put("caption", dimension.getCaption());
                    result.put(dimension.getUniqueName(), s);
                    break;
                default:
                    s.put("type", "Standard");
                    s.put("caption", dimension.getCaption());
                    result.put(dimension.getUniqueName(), s);
                    break;
                }
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid identifier");
        }
        return result;
    }

    /**
     * Returns all the hierarchies from a dimension specified in from.
     * 
     * @param from
     * @return the hierarchies of a specific dimension specified in from
     * @throws Solap4pyException
     */
    private JSONObject getHierarchies(JSONArray from) throws Solap4pyException, JSONException {
        JSONObject result = new JSONObject();
        try {
            Cube cube = this.extractCube(from);
            Dimension dimension = this.extractDimension(from, cube);
            
            List<Hierarchy> hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                JSONObject s = new JSONObject();
                s.put("caption", hierarchy.getCaption());
                result.put(hierarchy.getUniqueName(), s);
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid identifier");
        } 
        return result;
    }

    /**
     * Returns all the levels from a hierarchy specified in from, with its properties or not.
     *
     * @param from
     * @param withProperties
     *            if it returns the properties of the levels
     * @return the levels of specific hierarchy specified in from
     * @throws Solap4pyException
     */
    private JSONArray getLevels(JSONArray from, boolean withProperties) throws Solap4pyException, JSONException {
        JSONArray result = new JSONArray();
        try {
            Cube cube = this.extractCube(from);
            Dimension dimension = this.extractDimension(from, cube);
            Hierarchy hierarchy = this.extractHierarchy(from, dimension);
            
            List<Level> levels = hierarchy.getLevels();
            for (Level level : levels) {
        	if (!level.getCaption().equals("(All)")) {
                    JSONObject s = new JSONObject();
                    s.put("caption", level.getCaption());
                    s.put("id", level.getUniqueName());
                    if (withProperties == true) {
                        s.put("list-properties", this.getLevelProperties(level));
                    }
                    if(level.getDepth() != 0) {
                	result.put(level.getDepth() - 1, s);
                    } else {
                	result.put(level.getDepth(), s);
                    }
        	}
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid identifier");
        }
        return result;
    }

    /**
     *  Returns all the members' names from a hierarchy specified in from, with its properties or not.
     * @param from
     * @param withProperties
     *            if it returns the properties of the members
     * @param granularity
     *            level of granularity to get the members
     * @return the members of a specific level specified in from
     * @throws Solap4pyException
     */
    private JSONObject getMembers(JSONArray from, boolean withProperties, int granularity) throws Solap4pyException, JSONException {
        JSONObject result = new JSONObject();
        try {
            Cube cube = this.extractCube(from);
            Dimension dimension = this.extractDimension(from, cube);
            Hierarchy hierarchy = this.extractHierarchy(from, dimension);
            Level level = this.extractLevel(from, hierarchy);
            int depth = level.getDepth() + granularity;
            if (depth >= hierarchy.getLevels().size()) {
            	throw new Solap4pyException(ErrorType.BAD_REQUEST, "Inexistant granularity");
            }
            
            String current = null;
            List<Member> tmp = level.getMembers();
            List<Member> members = new LinkedList<Member>();
            for (Member m : tmp) {
                if (!m.getUniqueName().equals(current)) {
                    members.add(m);
                    current = m.getUniqueName();
                }
            }

            if (from.length() == 6) {
              	if(granularity == 0){ 
              		JSONArray memberArray = null;
              		if(from.get(5) instanceof JSONArray){
              			memberArray = from.getJSONArray(5);              			
              		}else{
              			throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid Array of identifiers");             			
              		}
            		 
            		String memberName = null;
            		List<Member> list = new LinkedList<Member>();
              		for (Member member : tmp) {              	
              			for (int i=0; i<memberArray.length(); i++) {
              				
	                		memberName = memberArray.getString(i);
	                		
	                		if( member.getUniqueName().equals(memberName) ){
	                			list.add(member);
	                		}
	                	}
	                	members = list;      
              		}
              	}
              	else{
              		if((from.get(5) instanceof JSONArray)){
              			throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid Identifier"); 
              		}else{
	              		for (Member member : members) {              	
		                    if (member.getUniqueName().equals(from.getString(5))) {
		                        List<Member> memberArray = new LinkedList<Member>(Arrays.asList(member));
			                        for (int i = 0; i < granularity; i++) {
			                            List<Member> list = new LinkedList<Member>();
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
                }
            }
            
            for (Member member : members) {
                JSONObject s = new JSONObject();
                s.put("caption", member.getCaption());
                if ("[Measures]".equals(from.getString(2))) {
                    s.put("description", member.getDescription());
                }
                if (withProperties == true) {
                    this.getMemberProperties(from, member, s);
                }
                result.put(member.getUniqueName(), s);
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid identifier");
        }
        return result;
    }

    /**
     * Returns the properties of a level.
     * 
     * @param level
     * @return the property of level
     * @throws Solap4pyException
     */
    private JSONObject getLevelProperties(Level level) throws Solap4pyException, JSONException {
        JSONObject result = new JSONObject();
        List<Property> properties = level.getProperties();
        for (Property property : properties) {
            JSONObject s = new JSONObject();
            s.put("caption", property.getCaption());
            if (Metadata.USELESS_PROPERTIES.contains(property.getUniqueName()) == false) {
                if ("Geometry".equals(property.getCaption())) {
                    s.put("type", "Geometry");
                } else {
                    s.put("type", "Standard");
                }
                result.put(property.getName(), s);
            }
        }
        return result;
    }

    /**
     * Returns the properties of a member.
     * 
     * @param from
     * @param member
     * @param result
     *            the member's property gotten
     * @throws Solap4pyException
     */
    private void getMemberProperties(JSONArray from, Member member, JSONObject result) throws Solap4pyException, JSONException {
        try {
            for (Property property : member.getProperties()) {
                if (Metadata.USELESS_PROPERTIES.contains(property.getUniqueName()) == false) {
                    if ("Geometry".equals(property.getCaption())) {
                        result.put(property.getName(), this.getGeometry(from, member, property.getCaption()));
                    } else {
                        result.put(property.getName(), member.getPropertyFormattedValue(property));
                    }
                }
            }
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid identifier");
        }
    }

    /**
     * Returns a geometric property of a member.
     * @param from
     * @param member
     * @param geometricProperty
     * @return the geometry of member
     * @throws Solap4pyException
     */
    private String getGeometry(JSONArray from, Member member, String geometricProperty) throws Solap4pyException, JSONException {
        CellSet cellSet = null;
        try {
            OlapStatement statement = this.olapConnection.createStatement();
            String nameMember = member.getUniqueName();
            
            cellSet = statement.executeOlapQuery("with member [Measures].[geo] as " + nameMember + ".Properties(\"" + geometricProperty
                                                 + "\") select [Measures].[geo] ON COLUMNS from " + from.getString(1));
        } catch (OlapException | NullPointerException e) {
            LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
            throw new Solap4pyException(ErrorType.BAD_REQUEST, "Invalid identifier");
        }
        return cellSet.getCell(0).getFormattedValue();
    }


    /**
     * 
     * @param from
     * @return
     * @throws OlapException
     * @throws JSONException
     */
    private Cube extractCube(JSONArray from) throws OlapException, JSONException {
	List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
	Cube cube = null;
	for (Cube c : cubes) {
	    if (c.getUniqueName().equals(from.getString(1))) {
		cube = c;
		break;
	    }
	}
	return cube;
    }
    
    /**
     * 
     * @param from
     * @param cube
     * @return
     * @throws OlapException
     * @throws JSONException
     */
    private Dimension extractDimension(JSONArray from, Cube cube) throws OlapException, JSONException {
        List<Dimension> dimensions = cube.getDimensions();
        Dimension dimension = null;
        for (Dimension d : dimensions) {
            if (d.getUniqueName().equals(from.getString(2))) {
                dimension = d;
                break;
            }
        }
        return dimension;
    }
    
    /**
     * 
     * @param from
     * @param dimension
     * @return
     * @throws OlapException
     * @throws JSONException
     */
    private Hierarchy extractHierarchy(JSONArray from, Dimension dimension) throws OlapException, JSONException {
        List<Hierarchy> hierarchies = dimension.getHierarchies();
        Hierarchy hierarchy = null;
        for (Hierarchy h : hierarchies) {
            if (h.getUniqueName().equals(from.getString(3))) {
                hierarchy = h;
                break;
            }
        }
        return hierarchy;
    }
    
    /**
     * 
     * @param from
     * @param hierarchy
     * @return
     * @throws OlapException
     * @throws JSONException
     */
    private Level extractLevel(JSONArray from, Hierarchy hierarchy) throws OlapException, JSONException {
        List<Level> levels = hierarchy.getLevels();
        Level level = null;
        for (Level l : levels) {
            if (l.getUniqueName().equals(from.getString(4))) {
                level = l;
                break;
            }
        }
        return level;
    }
    
 }
