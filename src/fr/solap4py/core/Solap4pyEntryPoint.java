package fr.solap4py.core;

import py4j.GatewayServer;
import java.sql.SQLException;

public class Solap4pyEntryPoint {

    private Solap4py mySolap4py;

    public Solap4pyEntryPoint() throws ClassNotFoundException, SQLException {
        mySolap4py = Solap4py.getSolap4Object();
    }

    public Solap4py getSolap4py() {
        return mySolap4py;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        GatewayServer gatewayServer = new GatewayServer(new Solap4pyEntryPoint(), 25335);
        gatewayServer.start();
    }

}
