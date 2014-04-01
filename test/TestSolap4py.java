import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import solap4py.core.Solap4py;

import javax.json.Json;
import javax.json.JsonObject;


public class TestSolap4py {

	@Test
	public void selectTest(){
		Properties prop = new Properties();
		InputStream input = null;
 
		try {
			File f1 = new File("config.properties");
			if(f1.exists() && !f1.isDirectory()) {
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

	 		JsonObject model = Json.createObjectBuilder()
					   .add("schema", "Traffic")
					   .add("cube", Json.createObjectBuilder()
							   .add("name", "Traffic")
							   .add("measures", Json.createArrayBuilder()
									   .add("Quantity").add("Value"))
							   .add("dimension", Json.createObjectBuilder()
									   .add("name", "Time")
									   .add("range", false)
									   .add("id", Json.createArrayBuilder()
											   .add("2000").add("2009"))
									   .add("aggregation", false)
									   .add("dimension", Json.createObjectBuilder()
											   .add("name", "Geo")
											   .add("range", false)
											   .add("id", Json.createArrayBuilder()
													   .add("France"))
											   .add("hierarchy", "Name")
											   .add("aggregation", "region")
											   .add("measure", true)
											   .add("dimension", Json.createObjectBuilder()
													.add("name", "Product")
													.add("range", false)
													.add("id", Json.createArrayBuilder())
													.add("measure", true)
												)
										)
								)
						).build();
			String query = model.toString();
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
