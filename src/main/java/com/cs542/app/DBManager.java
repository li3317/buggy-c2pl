/**
 * 
 */
package com.cs542.app;

import java.io.PrintWriter;
import java.sql.*;
import java.util.logging.Logger;

public class DBManager {
//	private static Logger LOG = null;
//	static {
//		System.setProperty("java.util.logging.SimpleFormatter.format",
//				"[%1$tF %1$tT] [%4$-7s] %5$s %n");
//		LOG = Logger.getLogger(DBManager.class.getName());
//	}
	private static final Logger LOG = Logger.getLogger(DBManager.class.getName());

	private String driver;
	private String user;
	private String url;
	private String password;
	private Connection conn;
	private String tableName;
	public int siteId;
	Statement createTable;
	private PrintWriter pr;

	public DBManager(int id, PrintWriter pr) {
		driver = "com.mysql.cj.jdbc.Driver";
		user = null;
		password = null;
		url = null;	
		tableName = null;
		createTable = null;
		siteId = id;
		this.pr = pr;
	}

	public DBManager(String usr, String pwd, String dbn, int siteId) {
		driver = "com.mysql.cj.jdbc.Driver";
		user = usr;
		password = pwd;
		String db = dbn + String.valueOf(siteId);
		url = "jdbc:mysql://localhost:3306/" + db;
		tableName = null;
		this.siteId = siteId;
		createTable = null;
	}

	public Boolean connDataBase(String usr, String pwd, String dbn) {
		user = usr;
		password = pwd;
		url = "jdbc:mysql://localhost:3306/" + dbn;	
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			
			if (!conn.isClosed())
				return true;
		} catch (ClassNotFoundException e) {
			LOG.warning("[" + siteId + "] Class not found exception:" + e.getMessage());
			e.printStackTrace();
			return false;
		} catch (SQLException e) {
			LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public Boolean createTable() {
		tableName = "kvstore";
		String CreateTablesql = "create table " + tableName + " (vkey varchar(20), value varchar(20));";
		
		try {
			Statement statement = conn.createStatement();
			statement.executeUpdate("drop table if exists " + tableName);

			createTable = conn.createStatement();
			createTable.executeUpdate(CreateTablesql);
		} catch (SQLException e) {
			LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
			e.printStackTrace();
			return false;
		} finally {
			//DBManager.freeRsSt(null, stCreateTable);
		}
		return true;
	}

	public Boolean write(String key, String value) {
		Statement statement = null;
		try {
			statement = conn.createStatement();
			String query = "select value from kvstore where vkey = '" + key + "'";
			ResultSet resultset = statement.executeQuery(query);

			if(resultset.next()) {
				query = "update kvstore set value = '" + value + "' where vkey = '" + key + "'";
				LOG.info("[" + siteId + "] key " + key + " found in db");
			}
			else {
				query = "insert into kvstore values ('" + key + "', '" + value + "')";
				LOG.info("[" + siteId + "] insert key " + key);
			}
			LOG.info(query);
			statement.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			freeRsSt(null, statement);
		}
		return true;
	}

	public String read(String key)
	{
		String val = "";
		String Querysql = "select value from kvstore where vkey = '" + key + "'";
		PreparedStatement psQuery = null;
		ResultSet rs = null;
		try {
			psQuery = conn.prepareStatement(Querysql); // TODO: difference between createStatement and prepareStatement?
			rs = psQuery.executeQuery();
			if (rs.next())
				val = rs.getString("value");
			return val;
		} catch (SQLException e) {
			LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
			e.printStackTrace();
			return "";
		} finally {
			freeRsSt(rs, psQuery);
		}
	}
	
	public void showAll() {
		String showTablesSql = "select * from kvstore";
		ResultSet rs = null;
		try {
			Statement showTables = conn.createStatement();
			rs = showTables.executeQuery(showTablesSql);

			String key;
			String value;
			while (rs.next()) {
				key = rs.getString("vkey");
				value = rs.getString("value");
				LOG.info("(" + siteId + ") [" + key + ": " + value + "]");
				pr.println("[" + key + ": " + value + "]");
			}
		} catch (SQLException e) {
			LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
			e.printStackTrace();
		}
	}

	public void freeRsSt(ResultSet rs, Statement st) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
				e.printStackTrace();
			} finally {
				if (st != null) {
					try {
						st.close();
					} catch (SQLException e) {
						LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
						e.printStackTrace();
					} 
				}
			}
		}
	}

	public Boolean disConn() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.warning("[" + siteId + "] SQL exception:" + e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
}
