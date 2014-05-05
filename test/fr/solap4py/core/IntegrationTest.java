package fr.solap4py.core;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.olap4j.OlapConnection;

public class IntegrationTest {

    private Solap4py solap4py;
    private OlapConnection olapConnection;

    @Before
    public void setUp() throws Exception {

        solap4py = Solap4py.getSolap4Object();
        this.olapConnection = solap4py.getOlapConnection();
    }

    @After
    public void tearDown() throws Exception {
        olapConnection.close();
    }

    @Test
    public void testSchemas() {
        String query = "{\"queryType\":\"metadata\",\"data\":{ \"root\" :[\"Traffic\"]}}";
        String result = this.solap4py.process(query);
        assertTrue(result.equals("{\"error\":\"OK\",\"data\":{\"[Traffic]\":{\"caption\":\"Traffic\"}}}"));
    }

    @Test
    public void testMembers() {
        String query = "{\"queryType\":\"metadata\",\"data\":{\"root\":[\"Traffic\", \"[Traffic]\", \"[Zone]\", \"[Zone.Name]\", \"[Zone.Name].[Name0]\"], \"withProperties\": false }}";
        String result = this.solap4py.process(query);
        assertTrue(result.equals("{\"error\":\"OK\",\"data\":{\"[Zone.Name].[All Zone.Names].[Croatia]\":{\"caption\":\"Croatia\"},\"[Zone.Name].[All Zone.Names].[Switzerland]\":{\"caption\":\"Switzerland\"},\"[Zone.Name].[All Zone.Names].[Cyprus]\":{\"caption\":\"Cyprus\"},\"[Zone.Name].[All Zone.Names].[Portugal]\":{\"caption\":\"Portugal\"},\"[Zone.Name].[All Zone.Names].[France]\":{\"caption\":\"France\"},\"[Zone.Name].[All Zone.Names].[Italy]\":{\"caption\":\"Italy\"},\"[Zone.Name].[All Zone.Names].[United Kingdom]\":{\"caption\":\"United Kingdom\"},\"[Zone.Name].[All Zone.Names].[Finland]\":{\"caption\":\"Finland\"},\"[Zone.Name].[All Zone.Names].[Iceland]\":{\"caption\":\"Iceland\"},\"[Zone.Name].[All Zone.Names].[Slovakia]\":{\"caption\":\"Slovakia\"},\"[Zone.Name].[All Zone.Names].[Belgium]\":{\"caption\":\"Belgium\"},\"[Zone.Name].[All Zone.Names].[Luxembourg]\":{\"caption\":\"Luxembourg\"},\"[Zone.Name].[All Zone.Names].[Turkey]\":{\"caption\":\"Turkey\"},\"[Zone.Name].[All Zone.Names].[Norway]\":{\"caption\":\"Norway\"},\"[Zone.Name].[All Zone.Names].[Slovenia]\":{\"caption\":\"Slovenia\"},\"[Zone.Name].[All Zone.Names].[Austria]\":{\"caption\":\"Austria\"},\"[Zone.Name].[All Zone.Names].[Romania]\":{\"caption\":\"Romania\"},\"[Zone.Name].[All Zone.Names].[Czech Republic]\":{\"caption\":\"Czech Republic\"},\"[Zone.Name].[All Zone.Names].[Malta]\":{\"caption\":\"Malta\"},\"[Zone.Name].[All Zone.Names].[Lithuania]\":{\"caption\":\"Lithuania\"},\"[Zone.Name].[All Zone.Names].[Denmark]\":{\"caption\":\"Denmark\"},\"[Zone.Name].[All Zone.Names].[Estonia]\":{\"caption\":\"Estonia\"},\"[Zone.Name].[All Zone.Names].[The former Yugoslav Republic of Macedonia]\":{\"caption\":\"The former Yugoslav Republic of Macedonia\"},\"[Zone.Name].[All Zone.Names].[Hungary]\":{\"caption\":\"Hungary\"},\"[Zone.Name].[All Zone.Names].[Latvia]\":{\"caption\":\"Latvia\"},\"[Zone.Name].[All Zone.Names].[Germany]\":{\"caption\":\"Germany\"},\"[Zone.Name].[All Zone.Names].[Bulgaria]\":{\"caption\":\"Bulgaria\"},\"[Zone.Name].[All Zone.Names].[Sweden]\":{\"caption\":\"Sweden\"},\"[Zone.Name].[All Zone.Names].[Greece]\":{\"caption\":\"Greece\"},\"[Zone.Name].[All Zone.Names].[Netherlands]\":{\"caption\":\"Netherlands\"},\"[Zone.Name].[All Zone.Names].[Liechtenstein]\":{\"caption\":\"Liechtenstein\"},\"[Zone.Name].[All Zone.Names].[Ireland]\":{\"caption\":\"Ireland\"},\"[Zone.Name].[All Zone.Names].[Poland]\":{\"caption\":\"Poland\"},\"[Zone.Name].[All Zone.Names].[Spain]\":{\"caption\":\"Spain\"}}}"));
    }

    @Test
    public void testInvalidSchema() {
        String query = "{ \"queryType\" : \"metadata\", \"data\" : { \"root\" : [\"Trafficv\"]}}";
        String result = this.solap4py.process(query);
        assertTrue(result.equals("{\"error\":\"BAD_REQUEST\",\"data\":\"Invalid schema identifier\"}"));
    }

    @Test
    public void testRange() {
        String query = "{ \"queryType\" : \"data\", \"data\" : { \"from\":\"[Traffic]\", \"onColumns\":[\"[Measures].[Max Quantity]\"], \"onRows\":{\"[Time]\":{\"members\":[\"[Time].[All Times].[2000]\", \"[Time].[All Times].[2003]\"], \"range\":true}}}}";
        String result = this.solap4py.process(query);
        assertTrue(result.equals("{\"error\":\"OK\",\"data\":[{\"[Measures].[Max Quantity]\":311121,\"[Time]\":\"[Time].[All Times].[2000]\"},{\"[Measures].[Max Quantity]\":304574,\"[Time]\":\"[Time].[All Times].[2001]\"},{\"[Measures].[Max Quantity]\":310543,\"[Time]\":\"[Time].[All Times].[2002]\"},{\"[Measures].[Max Quantity]\":315811,\"[Time]\":\"[Time].[All Times].[2003]\"}]}"));
    }

    @Test
    public void testNullMeasure() {
        String query = "{\"queryType\":\"data\",\"data\":{\"from\":\"[Traffic]\",\"onColumns\":[\"[Measures].[Goods Quantity]\"],\"onRows\":{\"[Time]\":{\"members\":[\"[Time].[All Times].[1950]\"], \"range\":false}}}}";
        String result = this.solap4py.process(query);
        assertTrue(result.equals("{\"error\":\"OK\",\"data\":[{\"[Measures].[Goods Quantity]\":0,\"[Time]\":\"[Time].[All Times].[1950]\"}]}"));
    }

}
