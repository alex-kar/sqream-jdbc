package com.sqream.jdbc;

import com.sun.jndi.toolkit.url.Uri;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class UriExTest {
    private static final String MINIMAL_URI = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String FULL_URI =
            "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=true;ssl=false;service=service;" +
                    "showFullStackTrace=true";
    private static final String FULL_URI_WITH_PARAMS = FULL_URI + "?loggerLevel=DEBUG";

    @Test
    public void fullUriTest() throws SQLException {
        checkArgs(new UriEx(FULL_URI));
    }

    @Test
    public void fullUriWithParamsTest() throws SQLException {
        checkArgs(new UriEx(FULL_URI_WITH_PARAMS));
    }

    @Test
    public void checkDefaultValuesTest() throws SQLException {
        UriEx uriEx = new UriEx(MINIMAL_URI);

        assertEquals("sqream", uriEx.getService());
        assertEquals("public", uriEx.getSchema());
    }

    private void checkArgs(UriEx uriEx) {
        assertEquals("sqream", uriEx.getProvider().toLowerCase());
        assertEquals("127.0.0.1", uriEx.getHost());
        assertEquals(5000, uriEx.getPort());
        assertEquals("master", uriEx.getDbName());
        assertEquals("sqream", uriEx.getUser());
        assertEquals("sqream", uriEx.getPswd());
        assertEquals("service", uriEx.getService());
        assertEquals("true", uriEx.getShowFullStackTrace());
        assertTrue(uriEx.getCluster());
        assertFalse(uriEx.getSsl());
    }
}