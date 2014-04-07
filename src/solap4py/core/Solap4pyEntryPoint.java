package solap4py.core;

import py4j.GatewayServer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Solap4pyEntryPoint {

    private Solap4py mySolap4py;

    public Solap4pyEntryPoint(String host, String port, String user, String passwd) {
        mySolap4py = new Solap4py(host, port, user, passwd);
    }

    public Solap4py getSolap4py() {
        return mySolap4py;
    }

    public static void main(String[] args) {
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

            GatewayServer gatewayServer = new GatewayServer(new Solap4pyEntryPoint(dbhost, dbport, dbuser, dbpasswd), 25336);
            gatewayServer.start();

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
