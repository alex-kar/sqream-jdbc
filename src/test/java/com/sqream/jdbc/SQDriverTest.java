package com.sqream.jdbc;

import org.junit.Test;

import java.net.ConnectException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.junit.Assert.*;

public class SQDriverTest {
    private static final String CORRECT_URI = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String LOWER_CASE_PROVIDER_URI = "jdbc:sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String ANOTHER_PROVIDER_URI = "jdbc:SomeProvider://127.0.0.1:5000/master;user=sqream;password=sqream";

    private static final int MINOR_VERSION = 0;
    private static final int MAJOR_VERSION = 4;

    private static final Properties CORRECT_CONN_PROPERTIES = new Properties();

    // init connection properties
    static {
        CORRECT_CONN_PROPERTIES.setProperty("user", "sqream");
        CORRECT_CONN_PROPERTIES.setProperty("password", "password");
    }

    @Test
    public void whenCorrectUriAcceptsURLReturnTrue() throws SQLException {
        assertTrue(new SQDriver().acceptsURL(CORRECT_URI));
    }

    @Test
    public void whenUriIsNullAcceptsURLReturnFalse() throws SQLException {
        assertTrue(new SQDriver().acceptsURL(null));
    }

    @Test
    public void whenUriIsEmptyAcceptsURLReturnFalse() throws SQLException {
        assertTrue(new SQDriver().acceptsURL(""));
    }

    @Test
    public void whenAnotherProviderInUriAcceptsURLReturnFalse() throws SQLException {
        assertFalse(new SQDriver().acceptsURL(ANOTHER_PROVIDER_URI));
    }

    @Test
    public void getMinorVersionTest() {
        assertEquals(MINOR_VERSION, new SQDriver().getMinorVersion());
    }

    @Test
    public void getMajorVersionTest() {
        assertEquals(MAJOR_VERSION, new SQDriver().getMajorVersion());
    }

    @Test
    public void jdbcCompliantTest() {
        assertTrue(new SQDriver().jdbcCompliant());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getParentLoggerTest() throws SQLFeatureNotSupportedException {
        new SQDriver().getParentLogger();
    }

    @Test
    public void getPropertyInfoTest() throws SQLException {
        DriverPropertyInfo[] propertyInfo = new SQDriver().getPropertyInfo(null, null);
        assertEquals(0, propertyInfo.length);
    }

    @Test(expected = SQLException.class)
    public void whenProviderLowerCaseConnectThrowExceptionTest() throws SQLException {
        new SQDriver().connect(LOWER_CASE_PROVIDER_URI, CORRECT_CONN_PROPERTIES);
    }

    @Test(expected = SQLException.class)
    public void whenPropertiesIsNullConnectThrowExceptionTest() throws SQLException {
        new SQDriver().connect(CORRECT_URI, null);
    }

    @Test(expected = ConnectException.class)
    public void whenPropertiesDoNotContainUserAndPasswordConnectReturnConnectionTest() throws SQLException {
        new SQDriver().connect(CORRECT_URI, new Properties());
    }


}