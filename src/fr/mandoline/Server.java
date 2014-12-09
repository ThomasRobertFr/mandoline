package fr.mandoline;

import fr.mandoline.core.*;

import java.io.*;

import org.json.JSONObject;
import org.json.JSONException;

import java.util.Properties;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.net.ServerSocket;
import java.net.Socket;

public class Server {
	private static final Logger LOGGER = Logger.getLogger("Mandoline Server");

	private static String GMHost, GMPort, GMName;

	private static int listenPort;

	/**
	 * Read parameters from a config file into static variables
	 */
	public static void readConfigFile() {
		Properties prop = new Properties();
		InputStream input = null;

		try {
			File f1 = new File("mandoline.properties");
			if (f1.exists() && !f1.isDirectory()) {
				input = new FileInputStream(f1);
			} else {
				input = new FileInputStream("config.dist");
			}

			// load a properties file
			prop.load(input);

			// get the property value
			Server.GMHost = prop.getProperty("dbhost");
			Server.GMPort = prop.getProperty("dbport");
			Server.GMName = prop.getProperty("geomondrianName");

			Server.listenPort = Integer.parseInt(prop.getProperty("listenPort"));

		} catch (IOException ex) {
			LOGGER.log(Level.SEVERE, ex.getMessage());
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage());
				}
			}
		}
	}

	private static class ClientThread extends Thread {
		Socket socket;

		public ClientThread(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			Mandoline mandoline;
			JSONObject jsonQuery;

			try {
				LOGGER.log(Level.INFO, "Connection accepted");
				BufferedReader in = new BufferedReader(
					new InputStreamReader(
						socket.getInputStream()
					)
				);
				PrintWriter out = new PrintWriter(
					socket.getOutputStream()
				);

				jsonQuery = new JSONObject(in.readLine());

				try {
					if (jsonQuery.has("role")) {
						mandoline = MandolinesManager.getMandoline(jsonQuery.getString("role"));
					} else {
						mandoline = MandolinesManager.getMandoline();
					}
					out.print(mandoline.process(jsonQuery));
				} catch (MandolineException me) {
					out.print(me.getJSON());
				}

				out.flush();
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
				//LOGGER.log(Level.SEVERE, e.getMessage());
			} finally {
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException socketNotFoundException) {
						LOGGER.log(Level.SEVERE, socketNotFoundException.getMessage());
					}
				}
			}
		}
	}

	public static void main(String[] args) {
		ServerSocket server = null;

		try {
			// Load configuration
			readConfigFile(); 

			// Initialize a mandolines manager with the parameters
			MandolinesManager.init(Server.GMHost, Server.GMPort, Server.GMName);

			// Load the xmla driver
			Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");

			// Start the server
			server = new ServerSocket(Server.listenPort);
			LOGGER.log(Level.INFO, "Listenning on port: " + Server.listenPort);

			while (true) {
				// Wait for incoming connections
				try {
					new ClientThread(server.accept()).start();
				} catch (IOException acceptClientException) {
					LOGGER.log(Level.SEVERE, acceptClientException.getMessage());
				}
			}
		} catch (Exception e) {
			// If an exception hits the main loop, print the message and exit
			//LOGGER.log(Level.SEVERE, e.getMessage());
			e.printStackTrace();
		} finally {
			if (server != null) {
				try {
					server.close();
				} catch (IOException socketNotFoundException) {
					LOGGER.log(Level.SEVERE, socketNotFoundException.getMessage());
				}
			}
		}
	}
}
