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
import org.olap4j.OlapException;
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

		query = Metadata.class.getDeclaredMethod("query", JSONObject.class,
				JSONObject.class);
		query.setAccessible(true);

		getSchemas = Metadata.class.getDeclaredMethod("getSchemas");
		getSchemas.setAccessible(true);

		getCubes = Metadata.class
				.getDeclaredMethod("getCubes", JSONArray.class);
		getCubes.setAccessible(true);

		getDimensions = Metadata.class.getDeclaredMethod("getDimensions",
				JSONArray.class);
		getDimensions.setAccessible(true);

		getHierarchies = Metadata.class.getDeclaredMethod("getHierarchies",
				JSONArray.class);
		getHierarchies.setAccessible(true);

		getLevels = Metadata.class.getDeclaredMethod("getLevels",
				JSONArray.class, boolean.class);
		getLevels.setAccessible(true);

		getMembers = Metadata.class.getDeclaredMethod("getMembers",
				JSONArray.class, boolean.class, int.class);
		getMembers.setAccessible(true);

		getLevelProperties = Metadata.class.getDeclaredMethod(
				"getLevelProperties", Level.class);
		getLevelProperties.setAccessible(true);

		getMemberProperties = Metadata.class.getDeclaredMethod(
				"getMemberProperties", JSONArray.class, Member.class,
				JSONObject.class);
		getMemberProperties.setAccessible(true);

		getGeometry = Metadata.class.getDeclaredMethod("getGeometry",
				JSONArray.class, Member.class, String.class);
		getGeometry.setAccessible(true);
	}

	@After
	public void tearDown() throws Exception {
		olapConnection.close();
	}

	@Test
	public void testGetSchemas() {

		try {
			JSONObject jsonObjectTest = (JSONObject) (getSchemas
					.invoke(metadata));

			assertTrue("getJSONCube did not return the list of Schemas ",
					jsonObjectTest.has("Traffic"));
			assertEquals("getJSONCube did not return the Schemas captions ",
					"Traffic", jsonObjectTest.getJSONObject("Traffic")
							.getString("caption"));

		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | JSONException e) {
			e.printStackTrace();
		}
	}

	@Test(expected = Solap4pyException.class)
	public void testGetCubes() throws Throwable {

		try {
			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\"]}}";

			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = (JSONObject) (getCubes.invoke(metadata, root));

			assertTrue("the result does not contain the first cube name",
					result.has("[Traffic]"));

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		String param = "{ \"queryType\" : \"metadata\","
				+ "\"data\" : { \"root\" : []}}";
		try {
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = (JSONObject) (getCubes.invoke(metadata, root));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testGetDimensions() throws Throwable {

		try {
			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\"]}}";
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = (JSONObject) (getDimensions.invoke(metadata,
					root));

			assertTrue("the result does not contain the first  Dimension name",
					result.has("[Zone]"));
			assertEquals("the Dimension Zone does not have the type Geometry",
					result.getJSONObject("[Zone]").getString("type"),
					"Geometry");
			assertTrue("the result does not contain the second Dimension name",
					result.has("[Measures]"));
			assertEquals("the Dimension Zone does not have the type Geometry",
					result.getJSONObject("[Measures]").getString("type"),
					"Measure");
			assertTrue("the result does not contain the third  Dimension name",
					result.has("[Time]"));
			assertEquals("the Dimension Zone does not have the type Geometry",
					result.getJSONObject("[Time]").getString("type"), "Time");

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		try {
			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", ]}}";
			JSONObject query;

			query = new JSONObject(param);

			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = (JSONObject) (getDimensions.invoke(metadata,
					root));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testgetHierarchies() throws Throwable {

		try {

			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\"]}}";
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = (JSONObject) (getHierarchies.invoke(metadata,
					root));

			assertTrue("the result does not contain the first hierarchy name",
					result.has("[Zone.Name]"));
			assertTrue("the result does not contain the second hierarchy name",
					result.has("[Zone.Reference]"));

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		try {

			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\",]}}";
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = (JSONObject) (getHierarchies.invoke(metadata,
					root));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test(expected = Solap4pyException.class)
	public void testGetLevels() throws Throwable {

		try {

			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\" : false}}";
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONArray result = (JSONArray) (getLevels.invoke(metadata, root,
					false));

			assertTrue("the result does not contain the first level", result
					.getJSONObject(0).get("id").equals("[Zone.Name].[(All)]"));
			assertTrue("the result does not contain the second level", result
					.getJSONObject(1).get("id").equals("[Zone.Name].[Name0]"));
			assertTrue("the result does not contain the third level", result
					.getJSONObject(2).get("id").equals("[Zone.Name].[Name1]"));
			assertTrue("the result does not contain the fourth level", result
					.getJSONObject(3).get("id").equals("[Zone.Name].[Name2]"));
			assertTrue("the result does not contain the fifth level", result
					.getJSONObject(4).get("id").equals("[Zone.Name].[Name3]"));

			result = (JSONArray) (getLevels.invoke(metadata, root, true));

			assertTrue("the first level does not retrieve his properties",
					result.getJSONObject(0).has("list-properties"));
			assertTrue("the second level does not retrieve his properties",
					result.getJSONObject(1).has("list-properties"));
			assertTrue("the third level does not retrieve his properties",
					result.getJSONObject(2).has("list-properties"));
			assertTrue("the fourth level does not retrieve his properties",
					result.getJSONObject(3).has("list-properties"));
			assertTrue("the fifth level does not retrieve his properties",
					result.getJSONObject(4).has("list-properties"));

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		try {
			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", 36416345, 251325], \"withProperties\" : false}}";
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONArray result = (JSONArray) (getLevels.invoke(metadata, root,
					false));
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test
	public void testGetLevelProperties() {

		try {
			Level levelTest = olapConnection.getOlapCatalog().getSchemas()
					.get("Traffic").getCubes().get("Traffic").getDimensions()
					.get("Zone").getHierarchies().get("Zone.Name").getLevels()
					.get("Name3");

			JSONObject result = (JSONObject) (getLevelProperties.invoke(
					metadata, levelTest));

			assertTrue(
					"the result does not contain the property Traffic Cube - Zone.Name Hierarchy - Name3 Level - Geom Property",
					result.has("Traffic Cube - Zone.Name Hierarchy - Name3 Level - Geom Property"));
			assertEquals(
					"the result does not contain a geometric property with the Geometry type",
					result.getJSONObject(
							"Traffic Cube - Zone.Name Hierarchy - Name3 Level - Geom Property")
							.get("type"), "Geometry");

		} catch (JSONException | IllegalAccessException
				| IllegalArgumentException | OlapException
				| InvocationTargetException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testGetMemberProperties() {
		String param = "{ \"queryType\" : \"metadata\","
				+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\" : true, \"granularity\" : 1}}";
		try {
			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = new JSONObject();
			Member memberTest = olapConnection.getOlapCatalog().getSchemas()
					.get("Traffic").getCubes().get("Traffic").getDimensions()
					.get("Zone").getHierarchies().get("Zone.Name").getLevels()
					.get("Name3").getMembers().get(1);

			getMemberProperties.invoke(metadata, root, memberTest, result);

			String geometry = "MULTIPOLYGON (((16.37251205000007 47.642524250000065, 16.351879550000092 47.67124675000008, 16.356693050000047 47.67953125000008, 16.333642550000036 47.678550750000056, 16.30900955000004 47.68716075000003, 16.310845550000067 47.69342375000008, 16.30471655000008 47.69721025000007, 16.302523050000048 47.70695725000007, 16.29246455000009 47.718540750000045, 16.29110705000005 47.72928975000008, 16.29663005000009 47.73988675000004, 16.294370550000053 47.74449775000005, 16.29941555000005 47.74882425000004, 16.299341550000065 47.76118425000004, 16.293825050000066 47.768335250000064, 16.303498550000086 47.78519175000008, 16.278524050000044 47.792699250000055, 16.268747550000057 47.79107575000006, 16.26905005000009 47.796405750000076, 16.277293050000083 47.80979075000005, 16.30898755000004 47.80730875000006, 16.314738050000074 47.804771250000044, 16.30784255000009 47.80340725000008, 16.31087855000004 47.796879250000075, 16.307484050000085 47.79080875000005, 16.315620050000064 47.78459275000006, 16.32327255000007 47.78821325000007, 16.331716550000067 47.80062925000004, 16.343427550000058 47.80137925000008, 16.349928050000074 47.81020025000004, 16.34807305000004 47.815629250000086, 16.377184550000038 47.82731925000007, 16.37194005000009 47.83108075000007, 16.38802005000008 47.84256175000007, 16.34798505000009 47.86814625000005, 16.36948005000005 47.87026625000004, 16.38889155000004 47.881601250000074, 16.40029855000006 47.875972250000075, 16.40882255000008 47.88090175000008, 16.416217050000057 47.890437750000046, 16.409556550000048 47.89272225000008, 16.42280505000008 47.901927250000085, 16.41970905000005 47.90609375000008, 16.424461550000046 47.91169775000009, 16.419188550000058 47.91315375000005, 16.427081550000082 47.91562975000005, 16.42670705000006 47.91942675000007, 16.436868050000044 47.927406750000046, 16.466699050000045 47.93985825000004, 16.479247550000082 47.93735125000006, 16.489393050000047 47.94297775000007, 16.502763550000054 47.939602250000064, 16.518676550000066 47.94357025000005, 16.523473550000062 47.93465825000004, 16.552944550000063 47.91157225000006, 16.556432550000068 47.90221425000004, 16.57270955000007 47.893552250000084, 16.574009550000085 47.88627375000004, 16.580290050000087 47.88861575000004, 16.58977955000006 47.901143750000074, 16.632352050000065 47.929466750000074, 16.632975050000084 47.94536875000006, 16.65231205000009 47.949211750000075, 16.651635550000037 47.95573125000004, 16.659275050000076 47.96116375000008, 16.665137050000055 47.957113750000076, 16.69969455000006 47.96692725000008, 16.70121855000008 47.97379575000008, 16.69554855000007 47.985944250000045, 16.70143205000005 47.98769725000005, 16.70210005000007 48.011652250000054, 16.735029050000037 48.009629250000046, 16.745685050000077 48.01502275000007, 16.759884550000038 48.01366925000008, 16.794322050000062 48.022946750000074, 16.799668550000092 48.02894425000005, 16.82816155000006 48.02915875000008, 16.83798605000004 48.03283675000006, 16.837000550000084 48.03763575000005, 16.82875155000005 48.03888375000008, 16.84046255000004 48.045816750000085, 16.844964550000043 48.05493325000003, 16.85651855000009 48.05477425000004, 16.860164050000037 48.06324375000008, 16.874184550000052 48.072309250000046, 16.88232755000007 48.068222750000075, 16.889991550000047 48.07463375000003, 16.916713550000054 48.06762125000006, 16.920015050000075 48.06422225000006, 16.915212550000092 48.05670575000005, 16.920397550000075 48.05072825000008, 16.96905755000006 48.041055250000056, 16.975039550000076 48.03526625000006, 16.97186005000009 48.03611975000007, 16.96898205000008 48.02620125000004, 16.979235550000055 48.02345425000004, 16.987463050000088 48.03063725000004, 17.000171050000063 48.05667125000008, 17.016295050000053 48.07352075000006, 16.977840550000053 48.09506475000006, 16.968651050000062 48.09619525000005, 16.97108755000005 48.100569250000035, 16.976118050000082 48.10594375000005, 16.984890550000046 48.106441250000046, 16.98494805000007 48.11127675000006, 16.997858050000048 48.109086750000074, 17.004410550000046 48.09873925000005, 17.02016105000007 48.10562175000007, 17.028767550000055 48.10007575000009, 17.002549050000084 48.09269325000008, 17.035995550000052 48.084302250000064, 17.04141005000008 48.10166775000005, 17.06392105000009 48.109825250000085, 17.06201855000006 48.11324225000004, 17.06749755000004 48.11955325000008, 17.078399550000086 48.115600750000056, 17.074400050000065 48.110801250000065, 17.07880005000004 48.10639925000004, 17.09420005000004 48.099998750000054, 17.089199550000046 48.096999750000066, 17.094400550000046 48.09159825000006, 17.076200550000067 48.086200750000046, 17.07279955000007 48.081599750000066, 17.08000005000008 48.078602250000074, 17.073799050000048 48.07099925000006, 17.087799050000058 48.06560125000004, 17.06959905000008 48.05599925000007, 17.08539955000009 48.050597750000065, 17.088600050000082 48.045798750000074, 17.068599550000044 48.03179925000006, 17.09140005000006 48.02040125000008, 17.11160055000005 48.03179925000006, 17.128000050000082 48.021400750000055, 17.145200550000084 48.020801250000034, 17.162000550000073 48.007198250000044, 17.096201050000047 47.97119875000004, 17.11820055000004 47.961200750000046, 17.09140005000006 47.93479925000008, 17.113800050000066 47.92760075000007, 17.097999550000054 47.90679925000006, 17.105600550000077 47.899601250000046, 17.086399050000068 47.875000250000085, 17.079000550000046 47.877998750000074, 17.040800050000087 47.867199250000056, 17.018999050000048 47.86899975000006, 17.011400050000077 47.859000750000064, 17.05279905000009 47.83859975000007, 17.04800055000004 47.82899825000004, 17.062401050000062 47.82300225000006, 17.076200550000067 47.80879975000005, 17.052400550000073 47.79420075000007, 17.068399550000038 47.768398750000074, 17.07139955000008 47.72840125000005, 17.094400550000046 47.70859925000008, 16.913601050000068 47.688202250000074, 16.876600050000036 47.68880075000004, 16.872801050000078 47.69020125000009, 16.867800050000085 47.722198750000075, 16.857000550000066 47.71179925000007, 16.840200550000077 47.705200250000075, 16.829799550000075 47.68320125000008, 16.75000005000004 47.68199925000005, 16.72240055000009 47.72280075000003, 16.721599550000064 47.736000250000075, 16.653999550000037 47.74380125000005, 16.64040005000004 47.74940075000006, 16.635599050000053 47.75999875000008, 16.628000050000082 47.757400250000046, 16.610000550000052 47.76079975000005, 16.60000055000006 47.75640075000007, 16.593200550000063 47.75960175000006, 16.569000050000056 47.75199875000004, 16.549600550000036 47.75199875000004, 16.549999050000054 47.74380125000005, 16.537800050000044 47.737399750000066, 16.55340005000005 47.72280075000003, 16.54200005000007 47.712398250000035, 16.52199955000009 47.712799250000046, 16.51840055000008 47.70740075000003, 16.500999550000074 47.705001750000065, 16.48640055000004 47.69660175000007, 16.486000050000087 47.69179875000003, 16.47719955000008 47.69039875000004, 16.475599550000084 47.681598250000036, 16.462600550000047 47.68320125000008, 16.455400550000036 47.697200750000036, 16.448400550000088 47.696998250000036, 16.44459905000008 47.67459875000009, 16.422600050000085 47.66579825000008, 16.40373505000008 47.63656375000005, 16.384854050000058 47.632839250000075, 16.37251205000007 47.642524250000065)))";

			assertEquals(
					"the result does not contain the geometric property value in WKT format of Nordburgenland, the first member of Name3 ",
					result.get("Traffic Cube - Zone.Name Hierarchy - Name3 Level - Geom Property"),
					geometry);

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (OlapException e) {
			e.printStackTrace();
		}
	}

	@Test(expected = Solap4pyException.class)
	public void testGetMembers() throws Throwable {

		try {

			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\" : false, \"granularity\" : 1}}";

			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = new JSONObject();
			result = (JSONObject) (getMembers.invoke(metadata, root, false, 1));

			assertTrue("the result does not contain the first member",
					result.has("[Zone.Name].[All Zone.Names].[Croatia]"));
			assertTrue("the result does not contain the last member",
					result.has("[Zone.Name].[All Zone.Names].[Spain]"));

			result = (JSONObject) (getMembers.invoke(metadata, root, true, 1));
			assertTrue(
					"the result does not contain the first member's properties",
					result.getJSONObject(
							"[Zone.Name].[All Zone.Names].[Croatia]")
							.has("Traffic Cube - Zone.Name Hierarchy - Name0 Level - Geom Property"));
			assertTrue(
					" the result does not contain the last member's properties  ",
					result.getJSONObject("[Zone.Name].[All Zone.Names].[Spain]")
							.has("Traffic Cube - Zone.Name Hierarchy - Name0 Level - Geom Property"));

			param = "{ \"queryType\" : \"metadata\","
                    + "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\" ,   \"[Zone.Name].[All Zone.Names].[France]\" ] , \"withProperties\" : false, \"granularity\" : 1}}";
            
            root = new JSONObject(param).getJSONObject("data").getJSONArray("root");
            
            result = (JSONObject) (getMembers.invoke(metadata, root, false, 0));
            assertTrue("the result does not only contains the France member", !result.has("[Zone.Name].[All Zone.Names].[Spain]") && result.has("[Zone.Name].[All Zone.Names].[France]"));
            result = (JSONObject) (getMembers.invoke(metadata, root, false, 2));
            assertTrue("the result does not only contains the children of France with a granularity 2",  result.has("[Zone.Name].[All Zone.Names].[France].[DÉPARTEMENTS D'OUTRE-MER].[Guyane]") && !result.has("[Zone.Name].[All Zone.Names].[France]"));
            
              param = "{ \"queryType\" : \"metadata\","
                    + "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Measures]\", \"[Measures]\", \"[Measures].[MeasuresLevel]\"], \"withProperties\" : true, \"granularity\" : 1}}";
           
            
            result = (JSONObject) (getMembers.invoke(metadata, new JSONObject(param).getJSONObject("data").getJSONArray("root"), true, 1));
            
            assertTrue(" the result does not contain the members corresponding to the different measures ", result.has("[Measures].[Goods Quantity]") && result.has("[Measures].[Max Quantity]"));
			
			
			
			
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		String param = "{ \"queryType\" : \"metadata\","
				+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", 351351], \"withProperties\" : false, \"granularity\" : 1}}";

		JSONObject query;
		try {
			query = new JSONObject(param);

			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			JSONObject result = new JSONObject();
			result = (JSONObject) (getMembers.invoke(metadata, root, false, 1));

		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e.getCause();
		}

	}

	@Test
	public void testGetGeometry() {
		try {
			String param = "{ \"queryType\" : \"metadata\","
					+ "\"data\" : { \"root\" : [\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name3]\"], \"withProperties\" : True}}";

			JSONObject query = new JSONObject(param);
			JSONObject data = query.getJSONObject("data");
			JSONArray root = data.getJSONArray("root");

			Member memberTest = olapConnection.getOlapCatalog().getSchemas()
					.get("Traffic").getCubes().get("Traffic").getDimensions()
					.get("Zone").getHierarchies().get("Zone.Name").getLevels()
					.get("Name3").getMembers().get(1);

			String geometry = "MULTIPOLYGON (((16.37251205000007 47.642524250000065, 16.351879550000092 47.67124675000008, 16.356693050000047 47.67953125000008, 16.333642550000036 47.678550750000056, 16.30900955000004 47.68716075000003, 16.310845550000067 47.69342375000008, 16.30471655000008 47.69721025000007, 16.302523050000048 47.70695725000007, 16.29246455000009 47.718540750000045, 16.29110705000005 47.72928975000008, 16.29663005000009 47.73988675000004, 16.294370550000053 47.74449775000005, 16.29941555000005 47.74882425000004, 16.299341550000065 47.76118425000004, 16.293825050000066 47.768335250000064, 16.303498550000086 47.78519175000008, 16.278524050000044 47.792699250000055, 16.268747550000057 47.79107575000006, 16.26905005000009 47.796405750000076, 16.277293050000083 47.80979075000005, 16.30898755000004 47.80730875000006, 16.314738050000074 47.804771250000044, 16.30784255000009 47.80340725000008, 16.31087855000004 47.796879250000075, 16.307484050000085 47.79080875000005, 16.315620050000064 47.78459275000006, 16.32327255000007 47.78821325000007, 16.331716550000067 47.80062925000004, 16.343427550000058 47.80137925000008, 16.349928050000074 47.81020025000004, 16.34807305000004 47.815629250000086, 16.377184550000038 47.82731925000007, 16.37194005000009 47.83108075000007, 16.38802005000008 47.84256175000007, 16.34798505000009 47.86814625000005, 16.36948005000005 47.87026625000004, 16.38889155000004 47.881601250000074, 16.40029855000006 47.875972250000075, 16.40882255000008 47.88090175000008, 16.416217050000057 47.890437750000046, 16.409556550000048 47.89272225000008, 16.42280505000008 47.901927250000085, 16.41970905000005 47.90609375000008, 16.424461550000046 47.91169775000009, 16.419188550000058 47.91315375000005, 16.427081550000082 47.91562975000005, 16.42670705000006 47.91942675000007, 16.436868050000044 47.927406750000046, 16.466699050000045 47.93985825000004, 16.479247550000082 47.93735125000006, 16.489393050000047 47.94297775000007, 16.502763550000054 47.939602250000064, 16.518676550000066 47.94357025000005, 16.523473550000062 47.93465825000004, 16.552944550000063 47.91157225000006, 16.556432550000068 47.90221425000004, 16.57270955000007 47.893552250000084, 16.574009550000085 47.88627375000004, 16.580290050000087 47.88861575000004, 16.58977955000006 47.901143750000074, 16.632352050000065 47.929466750000074, 16.632975050000084 47.94536875000006, 16.65231205000009 47.949211750000075, 16.651635550000037 47.95573125000004, 16.659275050000076 47.96116375000008, 16.665137050000055 47.957113750000076, 16.69969455000006 47.96692725000008, 16.70121855000008 47.97379575000008, 16.69554855000007 47.985944250000045, 16.70143205000005 47.98769725000005, 16.70210005000007 48.011652250000054, 16.735029050000037 48.009629250000046, 16.745685050000077 48.01502275000007, 16.759884550000038 48.01366925000008, 16.794322050000062 48.022946750000074, 16.799668550000092 48.02894425000005, 16.82816155000006 48.02915875000008, 16.83798605000004 48.03283675000006, 16.837000550000084 48.03763575000005, 16.82875155000005 48.03888375000008, 16.84046255000004 48.045816750000085, 16.844964550000043 48.05493325000003, 16.85651855000009 48.05477425000004, 16.860164050000037 48.06324375000008, 16.874184550000052 48.072309250000046, 16.88232755000007 48.068222750000075, 16.889991550000047 48.07463375000003, 16.916713550000054 48.06762125000006, 16.920015050000075 48.06422225000006, 16.915212550000092 48.05670575000005, 16.920397550000075 48.05072825000008, 16.96905755000006 48.041055250000056, 16.975039550000076 48.03526625000006, 16.97186005000009 48.03611975000007, 16.96898205000008 48.02620125000004, 16.979235550000055 48.02345425000004, 16.987463050000088 48.03063725000004, 17.000171050000063 48.05667125000008, 17.016295050000053 48.07352075000006, 16.977840550000053 48.09506475000006, 16.968651050000062 48.09619525000005, 16.97108755000005 48.100569250000035, 16.976118050000082 48.10594375000005, 16.984890550000046 48.106441250000046, 16.98494805000007 48.11127675000006, 16.997858050000048 48.109086750000074, 17.004410550000046 48.09873925000005, 17.02016105000007 48.10562175000007, 17.028767550000055 48.10007575000009, 17.002549050000084 48.09269325000008, 17.035995550000052 48.084302250000064, 17.04141005000008 48.10166775000005, 17.06392105000009 48.109825250000085, 17.06201855000006 48.11324225000004, 17.06749755000004 48.11955325000008, 17.078399550000086 48.115600750000056, 17.074400050000065 48.110801250000065, 17.07880005000004 48.10639925000004, 17.09420005000004 48.099998750000054, 17.089199550000046 48.096999750000066, 17.094400550000046 48.09159825000006, 17.076200550000067 48.086200750000046, 17.07279955000007 48.081599750000066, 17.08000005000008 48.078602250000074, 17.073799050000048 48.07099925000006, 17.087799050000058 48.06560125000004, 17.06959905000008 48.05599925000007, 17.08539955000009 48.050597750000065, 17.088600050000082 48.045798750000074, 17.068599550000044 48.03179925000006, 17.09140005000006 48.02040125000008, 17.11160055000005 48.03179925000006, 17.128000050000082 48.021400750000055, 17.145200550000084 48.020801250000034, 17.162000550000073 48.007198250000044, 17.096201050000047 47.97119875000004, 17.11820055000004 47.961200750000046, 17.09140005000006 47.93479925000008, 17.113800050000066 47.92760075000007, 17.097999550000054 47.90679925000006, 17.105600550000077 47.899601250000046, 17.086399050000068 47.875000250000085, 17.079000550000046 47.877998750000074, 17.040800050000087 47.867199250000056, 17.018999050000048 47.86899975000006, 17.011400050000077 47.859000750000064, 17.05279905000009 47.83859975000007, 17.04800055000004 47.82899825000004, 17.062401050000062 47.82300225000006, 17.076200550000067 47.80879975000005, 17.052400550000073 47.79420075000007, 17.068399550000038 47.768398750000074, 17.07139955000008 47.72840125000005, 17.094400550000046 47.70859925000008, 16.913601050000068 47.688202250000074, 16.876600050000036 47.68880075000004, 16.872801050000078 47.69020125000009, 16.867800050000085 47.722198750000075, 16.857000550000066 47.71179925000007, 16.840200550000077 47.705200250000075, 16.829799550000075 47.68320125000008, 16.75000005000004 47.68199925000005, 16.72240055000009 47.72280075000003, 16.721599550000064 47.736000250000075, 16.653999550000037 47.74380125000005, 16.64040005000004 47.74940075000006, 16.635599050000053 47.75999875000008, 16.628000050000082 47.757400250000046, 16.610000550000052 47.76079975000005, 16.60000055000006 47.75640075000007, 16.593200550000063 47.75960175000006, 16.569000050000056 47.75199875000004, 16.549600550000036 47.75199875000004, 16.549999050000054 47.74380125000005, 16.537800050000044 47.737399750000066, 16.55340005000005 47.72280075000003, 16.54200005000007 47.712398250000035, 16.52199955000009 47.712799250000046, 16.51840055000008 47.70740075000003, 16.500999550000074 47.705001750000065, 16.48640055000004 47.69660175000007, 16.486000050000087 47.69179875000003, 16.47719955000008 47.69039875000004, 16.475599550000084 47.681598250000036, 16.462600550000047 47.68320125000008, 16.455400550000036 47.697200750000036, 16.448400550000088 47.696998250000036, 16.44459905000008 47.67459875000009, 16.422600050000085 47.66579825000008, 16.40373505000008 47.63656375000005, 16.384854050000058 47.632839250000075, 16.37251205000007 47.642524250000065)))";

			String result = (String) (getGeometry.invoke(metadata, root,
					memberTest, "Geom"));

			assertEquals(
					"the result is not the geometric property value in WKT format of Nordburgenland, the first member of Name3 ",
					result, geometry);

		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | JSONException | OlapException e) {
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
			result = (JSONObject) (getMembers.invoke(metadata, root, false, 1));

			assertTrue("the result does not contain the first member",
					result.has("[Zone.Name].[All Zone.Names].[Croatia]"));
			assertTrue("the result does not contain the last member",
					result.has("[Zone.Name].[All Zone.Names].[Spain]"));

			result = (JSONObject) (getMembers.invoke(metadata, root, true, 1));
			assertTrue(
					"the result does not contain the first member's properties",
					result.getJSONObject(
							"[Zone.Name].[All Zone.Names].[Croatia]")
							.has("Traffic Cube - Zone.Name Hierarchy - Name0 Level - Geom Property"));
			assertTrue(
					" the result does not contain the last member's properties  ",
					result.getJSONObject("[Zone.Name].[All Zone.Names].[Spain]")
							.has("Traffic Cube - Zone.Name Hierarchy - Name0 Level - Geom Property"));

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
