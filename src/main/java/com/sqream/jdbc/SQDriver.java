package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.lang.reflect.Field;
import javax.script.ScriptException;

import com.sqream.jdbc.Connector.ConnException;

// Logging
import java.util.Arrays;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.file.Files;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;


public class SQDriver implements java.sql.Driver {

	private SQConnectionFactory connectionFactory = new SQConnectionFactory();

	boolean logging = Connector.is_logging();
	Path SQDriver_log = Paths.get("/tmp/SQDriver.txt");
	boolean log(String line) throws SQLException {
		if (!logging)
			return true;
		
		try {
			Files.write(SQDriver_log, Arrays.asList(new String[] {line}), UTF_8, CREATE, APPEND);
		} catch (IOException e) {
			e.printStackTrace();
			throw new SQLException ("Error writing to SQDriver log");
		}
		
		return true;
	}

	private DriverPropertyInfo[] DPIArray;
	static {
		try {
			DriverManager.registerDriver(new SQDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		log("inside acceptsURL in SQDriver");
		UriEx uex = new UriEx(url); // parse the url to object.
		if (uex.getProvider() == null || !"sqream".equals(uex.getProvider().toLowerCase())) {
			return false; // cause it is an other provider, not us..
		}
		return true;
	}
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		
		log("inside connect in SQDriver");
		try {
			System.setProperty("file.encoding","UTF-8");
			Field charset = Charset.class.getDeclaredField("defaultCharset");
			charset.setAccessible(true);
			charset.set(null,null);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		String prfx = "jdbc:Sqream";

		if (!url.trim().substring(0, prfx.length()).equals(prfx))
			
			throw new SQLException("Wrong prefix for connection string. Should be jdbc:Sqream but got: " + url.trim().substring(0, prfx.length())); // assaf: don't try to this url, it was not ment for
							// sqream. (propegate it on..)

		if (info == null)
			throw new SQLException("Properties info is null");

		UriEx UEX = new UriEx(url.trim()); // parse the url to object.
		if (!UEX.getProvider().toLowerCase().equals("sqream")) {

			throw new SQLException("Bad provider in connection string. Should be sqream but got: " + UEX.getProvider().toLowerCase());
		}

		if (UEX.getUser() == null && info.getProperty("user") == null || UEX.getPswd() == null && info.getProperty("password") == null) {

			throw new SQLException("please apply user and password"); 
		}

		if (UEX.getUser() != null) // override the properties object according to
								// Razi.
		{
			info.put("user", UEX.getUser());
			info.put("password", UEX.getPswd());
		}
		// now cast it to JDBC Properties object (cause thats what the
		// DriverManager gives us as parameter):
		if (UEX.getDbName() == null || UEX.getDbName().equals(""))
			throw new SQLException("connection string : missing database name error");
		if (UEX.getHost() == null)
			throw new SQLException("connection string : missing host ip error");
		if (UEX.getPort() == -1)
			throw new SQLException("connection string : missing port error");

		info.put("dbname", UEX.getDbName());
		info.put("port", String.valueOf(UEX.getPort()));
		info.put("host", UEX.getHost());
		info.put("cluster", UEX.getCluster());
		info.put("ssl", UEX.getSsl());
		
		if(UEX.getService() != null)
			info.put("service", UEX.getService());

		Boolean logConfigEnabled = UEX.getLogger() != null ? Boolean.valueOf(UEX.getLogger()) : false;

		if (UEX.getShowFullStackTrace() != null)
			info.put("showFullStackTrace", UEX.getShowFullStackTrace());
		//System.out.println ("connection info: " + info);
		Connection SQC;
		try {
			SQC = connectionFactory.initConnection(info);
			//String[] lables = { "url", "info" };
			//String[] values = { url, info.toString() };

		} catch (NumberFormatException | IOException | ScriptException| NoSuchAlgorithmException | KeyManagementException | ConnException  e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
		
		return SQC;
	}

	@Override
	public int getMajorVersion() {
		try {
			log("inside getMajorVersion in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 4;
	}

	@Override
	public int getMinorVersion() {
		try {
			log("inside getMinorVersion in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		log("inside getPropertyInfo in SQDriver");

		DPIArray = new DriverPropertyInfo[0];

		return DPIArray;
	}

	@Override
	public boolean jdbcCompliant() {
		try {
			log("inside jdbcCompliant in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		try {
			log("inside getParentLogger in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new SQLFeatureNotSupportedException("getParentLogger in SQDriver");
	}

}
