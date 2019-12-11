package com.sqream.jdbc;

import java.io.IOException;
import java.net.ConnectException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.logging.*;
import java.lang.reflect.Field;
import javax.script.ScriptException;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.enums.LoggerLevel;

import java.nio.charset.Charset;

public class SQDriver implements java.sql.Driver {
	private static final Logger PARENT_LOGGER = Logger.getLogger("com.sqream.jdbc");
	private static final Logger LOGGER = Logger.getLogger(SQDriver.class.getName());
	private static final String PROP_KEY_LOGGER_LEVEL = "loggerLevel";

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
		setPropsLoggerLevel();
		setUrlLoggerLevel(url);

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

		// create connection
		Connector connector = null;

		UriEx UEX = new UriEx(url);
		try {
			connector =  ConnectorFactory.getFactory().initConnector(UEX.getHost(), UEX.getPort(),
					UEX.getCluster(), UEX.getSsl());
			connector.connect(UEX.getDbName(), UEX.getUser(), UEX.getPswd(), UEX.getService());
		} catch (KeyManagementException | ScriptException | NoSuchAlgorithmException | ConnException | IOException e) {
			throw new SQLException(e);
		}

		Connection SQC;
		try {
			SQC = new SQConnection(UEX, connector);
		} catch (NumberFormatException e) {
			throw new SQLException(e);
		}

		return SQC;
	}

	@Override
	public int getMajorVersion() {
		log("inside getMajorVersion in SQDriver");
		return 4;
	}

	@Override
	public int getMinorVersion() {
		log("inside getMinorVersion in SQDriver");
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
		log("inside jdbcCompliant in SQDriver");
		return true;
	}

	@Override
	public Logger getParentLogger() {
		log("inside getParentLogger in SQDriver");
		return PARENT_LOGGER;
	}

	private void log(String line) {
		LOGGER.log(Level.FINE, line);
	}

	private void setUrlLoggerLevel(String url) {
		ConsoleHandler consoleHandler = new ConsoleHandler();
		consoleHandler.setLevel(Level.ALL);
		PARENT_LOGGER.addHandler(consoleHandler);
		PARENT_LOGGER.setLevel(Level.OFF);

		int index = url.indexOf("?");
		if (index != -1) {
			String[] props = url.substring(index + 1).split("$");
			Arrays.stream(props)
					.filter(prop -> prop.split("=")[0].equals(PROP_KEY_LOGGER_LEVEL))
					.findFirst()
					.map(prop -> prop.split("=")[1])
					.ifPresent(this::setLevel);
		}
	}

	private void setPropsLoggerLevel() {
		String LOGGING_LEVEL = System.getProperty(PROP_KEY_LOGGER_LEVEL);
		if (LOGGING_LEVEL != null) {
			setLevel(LOGGING_LEVEL);
		}
	}

	private void setLevel(String loggerLevel) {
		switch (LoggerLevel.valueOf(loggerLevel.toUpperCase())) {
			case OFF:
				PARENT_LOGGER.setLevel(Level.OFF);
				break;
			case DEBUG:
				PARENT_LOGGER.setLevel(Level.FINE);
				break;
			case TRACE:
				PARENT_LOGGER.setLevel(Level.FINEST);
				break;
			default:
				StringJoiner supportedLevels = new StringJoiner(", ");
				Arrays.stream(LoggerLevel.values()).forEach(value -> supportedLevels.add(value.getValue()));
				throw new IllegalArgumentException(String.format(
						"Unsupported logging level: %s. Driver supports: %s", loggerLevel, supportedLevels));
		}
	}
}
