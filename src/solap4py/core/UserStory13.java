package solap4py.core;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;

public class UserStory13 {

	
	private OlapConnection olapConnection;
	
	public UserStory13(String host, String port, String user, String passwd){
		try {
			Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
			Connection connection = DriverManager
					.getConnection("jdbc:xmla:Server=http://" + user + ":" + passwd + "@" + host + ":" + port + "/geomondrian/xmla");
			this.olapConnection = connection.unwrap(OlapConnection.class);
	

		} catch (ClassNotFoundException e) {
			System.err.println(e);
		} catch (SQLException e) {
			System.err.println(e);
		}

	}
	
	public void geoInFile(String level, String member, String fileName){
		String geometry = null;
		
		try {
			
			OlapStatement statement = olapConnection.createStatement();		
			CellSet cellSet = statement.executeOlapQuery("with member [Measures].[test] as '[Zone.Name].[" + level + "].[" + member + "].Properties(\"Geom\")' select {[Measures].[test]} ON COLUMNS from [Traffic]");
		
			Cell cell = cellSet.getCell(0);
			geometry = cell.getFormattedValue();//.replace('(', '[').replace(')',']');
	
		   	FileWriter fw = new FileWriter(fileName, true);
					
		   	BufferedWriter output = new BufferedWriter(fw);
					
		   	output.write(geometry);
					
		   	output.flush();
		   	output.close();
		   	System.out.println("done.");
		        
		        
		            
		} catch (OlapException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();      		
		}
		
	}
	/*
	 	User Story 13 : getting the geometric dimension of any member
		args[0] : level of the  member
		args[1] : member 
		args[2] : name of output file
	 */
	
	public static void main(String[] args) {
		
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
			

			UserStory13 user = new UserStory13(dbhost, dbport, dbuser, dbpasswd);
			user.geoInFile(args[0], args[1], args[2]);	
		
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
