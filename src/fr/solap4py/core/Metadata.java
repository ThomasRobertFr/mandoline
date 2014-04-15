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
	private static final List<String> USELESS_PROPERTIES = new ArrayList<String>(
			Arrays.asList("CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME",
					"DIMENSION_UNIQUE_NAME", "HIERARCHY_UNIQUE_NAME",
					"LEVEL_UNIQUE_NAME", "LEVEL_NUMBER", "MEMBER_ORDINAL",
					"MEMBER_NAME", "MEMBER_UNIQUE_NAME", "MEMBER_TYPE",
					"MEMBER_GUID", "MEMBER_CAPTION", "CHILDREN_CARDINALITY",
					"PARENT_LEVEL", "PARENT_UNIQUE_NAME", "PARENT_COUNT",
					"DESCRIPTION", "$visible", "MEMBER_KEY",
					"IS_PLACEHOLDERMEMBER", "IS_DATAMEMBER", "DEPTH",
					"DISPLAY_INFO", "VALUE"));
	private static final Logger LOGGER = Logger.getLogger(Metadata.class
			.getName());

	public Metadata(OlapConnection connection) {
		try {
			this.catalog = connection.getOlapCatalog();
			this.olapConnection = connection;
		} catch (OlapException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
		}
	}

	public JSONObject query(JSONObject query, JSONObject jsonResult)
			throws Solap4pyException {
		JSONArray root = null;
		boolean withProperties;

		try {
			root = query.getJSONArray("root");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"'root' field not specified or invalid");
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
					throw new Solap4pyException(ErrorType.BAD_REQUEST,
							"'withProperties' field not specified or invalid");
				}
				jsonResult.put("data", this.getLevels(root, withProperties));
				break;
			case 5:
				try {
					withProperties = query.getBoolean("withProperties");
				} catch (JSONException e) {
					LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
					throw new Solap4pyException(ErrorType.BAD_REQUEST,
							"'withProperties' field not specified or invalid");
				}
				jsonResult
						.put("data", this.getMembers(root, withProperties, 0));
				break;
			case 6:
				try {
					withProperties = query.getBoolean("withProperties");
				} catch (JSONException e) {
					LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
					throw new Solap4pyException(ErrorType.BAD_REQUEST,
							"'withProperties' field not specified or invalid");
				}
				int granularity;
				try {
					granularity = query.getInt("granularity");
				} catch (JSONException e) {
					LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
					throw new Solap4pyException(ErrorType.BAD_REQUEST,
							"'granularity' field not specified or invalid");
				}
				if (granularity < 0) {
					throw new Solap4pyException(ErrorType.BAD_REQUEST,
							"'granularity' must be a positive integer'");
				}
				jsonResult.put("data",
						this.getMembers(root, withProperties, granularity));
				break;
			default:
				throw new Solap4pyException(ErrorType.BAD_REQUEST,
						"Too many parameters in array 'root'");
			}
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"An error occured while building json result");
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
		} catch (OlapException | NullPointerException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while trying to retrieve schemas");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}

		return result;
	}

	private JSONObject getCubes(JSONArray from) throws Solap4pyException {
		JSONObject result = new JSONObject();
		try {
			List<Cube> cubes = this.catalog.getSchemas().get(from.getString(0))
					.getCubes();

			for (Cube cube : cubes) {
				JSONObject s = new JSONObject();
				s.put("caption", cube.getCaption());
				result.put(cube.getUniqueName(), s);
			}
		} catch (OlapException | NullPointerException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid schema identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}
		return result;
	}

	private JSONObject getDimensions(JSONArray from) throws Solap4pyException {
		JSONObject result = new JSONObject();
		try {
			List<Cube> cubes = null;
			cubes = this.catalog.getSchemas().get(from.getString(0)).getCubes();
			Cube cube = null;
			for (Cube c : cubes) {
				if (c.getUniqueName().equals(from.getString(1))) {
					cube = c;
					break;
				}
			}

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
					Iterator<Property> i = dimension.getHierarchies().get(1)
							.getLevels().get(1).getMembers().get(0)
							.getProperties().iterator();
					Boolean hasGeometry = false;
					String type;
					while (i.hasNext() & !hasGeometry) {
						hasGeometry = "Geom".equals(i.next().getCaption()
								.substring(0, 4));
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
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}

		return result;
	}

	private JSONObject getHierarchies(JSONArray from) throws Solap4pyException {
		JSONObject result = new JSONObject();
		try {
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
			for (Hierarchy hierarchy : hierarchies) {
				JSONObject s = new JSONObject();
				s.put("caption", hierarchy.getCaption());
				result.put(hierarchy.getUniqueName(), s);
			}
		} catch (OlapException | NullPointerException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}

		return result;
	}

	private JSONArray getLevels(JSONArray from, boolean withProperties)
			throws Solap4pyException {
		JSONArray result = new JSONArray();
		try {
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
			for (Level level : levels) {
				JSONObject s = new JSONObject();
				s.put("caption", level.getCaption());
				s.put("id", level.getUniqueName());
				if (withProperties == true) {
					s.put("list-properties", this.getLevelProperties(level));
				}
				result.put(level.getDepth(), s);
			}
		} catch (OlapException | NullPointerException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}

		return result;
	}

	private JSONObject getMembers(JSONArray from, boolean withProperties,
			int granularity) throws Solap4pyException {
		JSONObject result = new JSONObject();
		try {
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

			if (from.length() == 6) {
				for (Member member : members) {
					if (member.getUniqueName().equals(from.getString(5))) {
						List<Member> memberArray = new LinkedList<Member>(
								Arrays.asList(member));
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
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}

		return result;
	}

	private JSONObject getLevelProperties(Level level) throws Solap4pyException {
		JSONObject result = new JSONObject();
		try {
			List<Property> properties = level.getProperties();
			for (Property property : properties) {
				JSONObject s = new JSONObject();
				s.put("caption", property.getCaption());
				if (Metadata.USELESS_PROPERTIES.contains(property
						.getUniqueName()) == false) {
					if ("Geom".equals(property.getCaption().substring(0, 4))) {
						s.put("type", "Geometry");
					} else {
						s.put("type", "Standard");
					}
					result.put(property.getUniqueName(), s);
				}
			}
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}
		return result;
	}

	private void getMemberProperties(JSONArray from, Member member,
			JSONObject result) throws Solap4pyException {
		try {
			for (Property property : member.getProperties()) {
				if (Metadata.USELESS_PROPERTIES.contains(property
						.getUniqueName()) == false) {
					if ("Geom".equals(property.getCaption().substring(0, 4))) {
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
		} catch (OlapException | NullPointerException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}
	}

	private String getGeometry(JSONArray from, Member member,
			String geometricProperty) throws Solap4pyException {
		CellSet cellSet = null;
		try {
			OlapStatement statement = this.olapConnection.createStatement();
			String nameMember = member.getUniqueName();
			cellSet = statement
					.executeOlapQuery("with member [Measures].[geo] as "
							+ nameMember + ".Properties(\"" + geometricProperty
							+ "\") select [Measures].[geo] ON COLUMNS from "
							+ from.getString(1));
		} catch (OlapException | NullPointerException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.BAD_REQUEST,
					"Invalid identifier");
		} catch (JSONException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
			throw new Solap4pyException(ErrorType.UNKNOWN_ERROR,
					"An error occured while building json result");
		}
		return cellSet.getCell(0).getFormattedValue();
	}

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, JSONException {

		String param = "{ \"root\" : [\"Traffic\"]}";

		Solap4py p = Solap4py.getSolap4Object();
		JSONObject query = new JSONObject(param);
		Metadata m = new Metadata(p.getOlapConnection());
		JSONObject result = new JSONObject();
		try {
			result = m.query(query, result);
			LOGGER.log(java.util.logging.Level.INFO, result.toString());
		} catch (Solap4pyException e) {
			LOGGER.log(java.util.logging.Level.SEVERE, e.getMessage());
		}

	}

}
