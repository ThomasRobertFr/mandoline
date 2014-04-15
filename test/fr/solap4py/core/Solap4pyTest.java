package fr.solap4py.core;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.olap4j.OlapConnection;

public class Solap4pyTest {

    private Solap4py solap4py;
    private OlapConnection olapConnection;
    private String s;

    @Before
    public void setUp() throws Exception {

        solap4py = Solap4py.getSolap4Object();
        olapConnection = solap4py.getOlapConnection();
        s = "{queryType: data, data: {" + "onColumns:" + "[" + "\"[Measures].[Goods Quantity]\"," + "\"[Measures].[Max Quantity]\"" + "],"
            + " onRows:" + "{"
            // + "\"[Time]\":{\"members\":[\"[2000]\"],\"range\":false} "
            + "}," + " where:" + "{" + "\"[Zone.Name]\":{\"members\":[\"[France]\"],\"range\":false} " + "}," + "from:" + "\"[Traffic]\""
            + "}}";

    }

    @After
    public void tearDown() throws Exception {
        olapConnection.close();
    }

    @Test
    public void testProcess() {

        String result = solap4py.process(s);

        try {
            JSONObject test = new JSONObject(result);

            assertTrue("The result does not have the key \"error\" ", test.has("error"));
            assertTrue("The result does not have the key \"data\" ", test.has("data"));

        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

}
