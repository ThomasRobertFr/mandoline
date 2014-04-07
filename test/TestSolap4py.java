import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import solap4py.core.Solap4py;

public class TestSolap4py {
    @Test
    public void selectTest() throws JSONException {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            File f1 = new File("config.properties");
            if (f1.exists() && !f1.isDirectory()) {
                input = new FileInputStream(f1);
            } else {
                input = new FileInputStream("config.dist");
            }

            // load a properties file
            prop.load(input);

            // get the property value
            String dbhost = prop.getProperty("dbhost");
            String dbuser = prop.getProperty("dbuser");
            String dbpasswd = prop.getProperty("dbpasswd");
            String dbport = prop.getProperty("dbport");
            Solap4py solap4py = new Solap4py(dbhost, dbport, dbuser, dbpasswd);

            JSONObject model = new JSONObject(); // Json.createObjectBuilder()
            model.put("schema", "Traffic")
                 .put("cube",
                      (new JSONObject()).put("name", "Traffic")
                                        .put("measures", (new JSONArray()).put("Quantity").put("Value"))
                                        .put("dimension",
                                             (new JSONObject()).put("name", "Time")
                                                               .put("range", false)
                                                               .put("id", (new JSONArray()).put("2000").put("2009"))
                                                               .put("aggregation", false)
                                                               .put("dimension",
                                                                    (new JSONObject()).put("name", "Geo")
                                                                                      .put("range", false)
                                                                                      .put("id", (new JSONArray()).put("France"))
                                                                                      .put("hierarchy", "Name")
                                                                                      .put("aggregation", "region")
                                                                                      .put("measure", true)
                                                                                      .put("dimension",
                                                                                           (new JSONObject()).put("name", "Product")
                                                                                                             .put("range", false)
                                                                                                             .put("id", new JSONArray())
                                                                                                             .put("measure", true)))));
            String query = model.toString();
            System.out.println(query);
            String res = solap4py.select(query);
            System.out.println(res);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
