package fr.mandoline;

import fr.mandoline.core.*;

import java.util.concurrent.ConcurrentHashMap;

import java.sql.SQLException;

public class MandolinesManager {
	private static ConcurrentHashMap<String, Mandoline> mandolines = new ConcurrentHashMap<String, Mandoline>();
	private static Mandoline defaultMandoline;
	private static String host, port, geomondrianName;

	/**
	 * Initialize the static MandolinesManager with GeoMondrian's connections informations.
	 *
	 * Additionnally, it creates a first default Mandoline object that will be used for roleless
	 * queries. It also tests the connection to GeoMondrian at startup.
	 *
	 * @param String GeoMondrian's host
	 * @param String GeoMondrian's port
	 * @param String GeoMondrian's webapp name
	 */
	public static void init(String host, String port, String geomondrianName) throws MandolineException, SQLException {
		MandolinesManager.host = host;
		MandolinesManager.port = port;
		MandolinesManager.geomondrianName = geomondrianName;

		defaultMandoline = new Mandoline(host, port, geomondrianName);
	}

	/**
	 * Get a mandoline object associated to the given GeoMondrian role.
	 *
	 * This method creates the mandoline object is needed, and is backed by a thread
	 * safe data structure.
	 *
	 * @param String GeoMondrian role
	 *
	 * @return Mandoline object
	 */
	public static Mandoline getMandoline(String role) throws MandolineException, SQLException {
		Mandoline m = mandolines.get(role);

		if (m == null) {
			m = new Mandoline(host, port, geomondrianName, role);
			mandolines.put(role, m);
		}

		return m;
	}

	/**
	 * Get a mandoline object without an associated role
	 *
	 * @return Mandoline object
	 */
	public static Mandoline getMandoline() throws MandolineException, SQLException {
		return MandolinesManager.defaultMandoline;
	}
}
