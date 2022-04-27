/**
 * Author: Chaoyang Liu
 * E-main: chaoyanglius@outlook.com
 *
 * Software License Agreement (GPL License)
 * A example using RMI & JDBC to operate remote database: Server Side
 * Copyright (c) 2017, Chaoyang Liu
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package com.cs542.app;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class C2plServer {

//	private static Logger LOG = null;
	private static final int TOTAL_SITES = 4;
	private static Config config;
	private static Map<Integer, Integer> ports;
	private static String url;
	private static List<DataSite> dataSites;
	private static final String TXNFILE = "transaction/transaction1.txt";

//	static {
//		System.setProperty("java.util.logging.SimpleFormatter.format",
//				"[%1$tF %1$tT] [%4$-7s] %5$s %n");
//		LOG = Logger.getLogger(C2plServer.class.getName());
//	}
	private static final Logger LOG = Logger.getLogger(C2plServer.class.getName());

	public static void main(String[] args) {
		// start a centralSite thread and 4 dataSite threads
		config = new Config("config/config.properties");
		url = config.getProperty("url");
		ports = new HashMap<>();
		dataSites = new ArrayList<>();

		if (url == null) {
			url = "localhost";
		}
		int portc = Integer.parseInt(config.getProperty("portc"));
		try {

			// set up centralSite first
			CentralSite cs = new CentralSite(portc, url, TOTAL_SITES);

			// TODO: set up sites and site id programmatically through a counter on central site
			for (int i = 1; i <= TOTAL_SITES; i++) {
				addSite(i, portc);
			}

			cs.setPorts(ports);

			dataSites.forEach(d -> {
				d.setUpParticipants(url, ports, TOTAL_SITES);
			});



			// start all threads
//			dataSites.forEach(ds -> {
//				ds.run();
//			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addSite(int id, int portc) throws RemoteException {
		int port = Integer.parseInt(config.getProperty("port" + id));
		ports.put(id, port);
		try {
			DataSite ds = new DataSite(id, port, url, TXNFILE, portc, TOTAL_SITES);
			dataSites.add(ds);
		} catch (RemoteException | FileNotFoundException | UnsupportedEncodingException e) {
			LOG.info("Remote Exception in starting server: " + e.getMessage());
			e.printStackTrace();
		}
	}

}
