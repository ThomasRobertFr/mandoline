package fr.solap4py.core;


import java.sql.SQLException;

import py4j.GatewayServer;

public class Solap4pyEntryPoint {

    private Solap4py mySolap4py;

    public Solap4pyEntryPoint(String host, String port, String user, String passwd) throws ClassNotFoundException, SQLException {
        mySolap4py = new Solap4py(host, port, user, passwd);
    }

    public Solap4py getSolap4py() {
        return mySolap4py;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Solap4py solap4py = Solap4py.getSolap4Object();

        GatewayServer gatewayServer = new GatewayServer(solap4py, 25335);
        gatewayServer.start();
    }

}
