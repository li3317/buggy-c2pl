package com.cs542.app;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class C2plTest {

    private static Logger LOG = null;
    private static Config config;
    private static Map<Integer, Integer> ports;
    private static String url;
    private static List<DataSite> dataSites;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        LOG = Logger.getLogger(C2plTest.class.getName());
    }

//    private static final Logger LOG = Logger.getLogger(C2plTest.class.getName());

    @Test
    public void testBasic() {
        config = new Config("config/config.properties");
        url = config.getProperty("url");
        ports = new HashMap<>();
        dataSites = new ArrayList<>();

        int TOTAL_SITES = 4;

        if (url == null) {
            url = "localhost";
        }
        int portc = Integer.parseInt(config.getProperty("portc"));
        try {

            // set up centralSite first
            CentralSite cs = new CentralSite(portc, url, TOTAL_SITES);
            System.out.println("here1");

            String txnfile;
            for (int i = 1; i <= TOTAL_SITES; i++) {
                txnfile = "transaction/test1-" + i + ".txt";
                addSite(i, txnfile, portc, TOTAL_SITES);
            }

            cs.setPorts(ports);
            dataSites.forEach(ds -> {
                ds.setUpParticipants(url, ports, TOTAL_SITES);
            });

            System.out.println("wtf");

            ExecutorService executor = Executors.newFixedThreadPool(TOTAL_SITES + 1);
            executor.execute(cs);


            dataSites.forEach(ds -> {
                executor.execute(ds);
            });

            executor.shutdown();
            while (!executor.isTerminated()) {
                // empty body
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTriggerDeadlock() {
        config = new Config("config/config.properties");
        url = config.getProperty("url");
        ports = new HashMap<>();
        dataSites = new ArrayList<>();

        int TOTAL_SITES = 2;

        if (url == null) {
            url = "localhost";
        }
        int portc = Integer.parseInt(config.getProperty("portc"));
        try {

            // set up centralSite first
            CentralSite cs = new CentralSite(portc, url, TOTAL_SITES);
            System.out.println("here1");

//            DataSite ds1 = addSite(1, "transaction/testT1.txt", portc, TOTAL_SITES);
//            DataSite ds2 = addSite(2, "transaction/testT2.txt", portc, TOTAL_SITES);

            DataSite ds1 = addSite(1, "transaction/test1-1.txt", portc, TOTAL_SITES);
            DataSite ds2 = addSite(2, "transaction/test1-1.txt", portc, TOTAL_SITES);

            cs.setPorts(ports);

//            dataSites.forEach(d -> {
//                d.setUpParticipants(url, ports, TOTAL_SITES);
//            });
            ds1.setUpParticipants(url, ports, TOTAL_SITES);
            ds2.setUpParticipants(url, ports, TOTAL_SITES);


            System.out.println("wtf");

            ExecutorService executor = Executors.newFixedThreadPool(3);
            executor.execute(cs);
            executor.execute(ds1);
            executor.execute(ds2);

            executor.shutdown();
            while (!executor.isTerminated()) {
                // empty body
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTriggerDeadlock4() {
        config = new Config("config/config.properties");
        url = config.getProperty("url");
        ports = new HashMap<>();
        dataSites = new ArrayList<>();

        int TOTAL_SITES = 4;

        if (url == null) {
            url = "localhost";
        }
        int portc = Integer.parseInt(config.getProperty("portc"));
        try {

            // set up centralSite first
            CentralSite cs = new CentralSite(portc, url, TOTAL_SITES);
            System.out.println("here1");

//            DataSite ds1 = addSite(1, "transaction/testT1.txt", portc, TOTAL_SITES);
//            DataSite ds2 = addSite(2, "transaction/testT2.txt", portc, TOTAL_SITES);

            DataSite ds1 = addSite(1, "transaction/test1-1.txt", portc, TOTAL_SITES);
            DataSite ds2 = addSite(2, "transaction/test1-2.txt", portc, TOTAL_SITES);
            DataSite ds3 = addSite(3, "transaction/test1-1.txt", portc, TOTAL_SITES);
            DataSite ds4 = addSite(4, "transaction/test1-2.txt", portc, TOTAL_SITES);

//            for (int i = 1; i <= TOTAL_SITES; i++) {
//                addSite(i, "transaction/test1-1.txt", portc, TOTAL_SITES);
//            }

            cs.setPorts(ports);

            dataSites.forEach(d -> {
                d.setUpParticipants(url, ports, TOTAL_SITES);
            });
//            ds1.setUpParticipants(url, ports, TOTAL_SITES);
//            ds2.setUpParticipants(url, ports, TOTAL_SITES);


            System.out.println("wtf");

            ExecutorService executor = Executors.newFixedThreadPool(TOTAL_SITES + 1);
            executor.execute(cs);
//            executor.execute(ds1);
//            executor.execute(ds2);
            dataSites.forEach(d -> {
                executor.execute(d);
            });

            executor.shutdown();
            while (!executor.isTerminated()) {
                // empty body
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataSite addSite(int id, String file, int portc, int totalSites) throws RemoteException {
        int port = Integer.parseInt(config.getProperty("port" + id));
        ports.put(id, port);
        DataSite ds = null;
        try {
            ds = new DataSite(id, port, url, file, portc, totalSites);
            dataSites.add(ds);
        } catch (RemoteException e) {
            LOG.info("Remote Exception in starting server: " + e.getMessage());
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return ds;
    }
}
