package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.socket.SQSocket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConnectorFactory.class, SQSocket.class})
public class SQResultSetMetaDataTest {

    @Test
    public void isCurrencyTest() throws
            ConnException, NoSuchAlgorithmException, IOException, KeyManagementException, SQLException {

        Connector connector = new ConnectorImpl(IP, PORT, CLUSTER, SSL);

        SQResultSetMetaData resultSetMetaData = new SQResultSetMetaData(connector, DATABASE);

        assertFalse(resultSetMetaData.isCurrency(1));
    }

    @Test
    public void columnDisplaySizeTest() throws SQLException {
        String createSql = "create or replace table test_display_size " +
                "(col1 bool, col2 tinyint, col3 smallint, col4 int, col5 bigint, col6 real, col7 double, " +
                "col8 varchar(10), col9 nvarchar(10), col10 text(10), col11 nvarchar, col12 text, col13 date, col14 datetime)";
        String selectSql = "select * from test_display_size";

        ResultSetMetaData rsmeta;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
            ResultSet rs = stmt.executeQuery(selectSql);
            rsmeta = rs.getMetaData();
            rs.close();
        }

        assertNotNull(rsmeta);
        assertEquals(1, rsmeta.getColumnDisplaySize(1));
        assertEquals(3, rsmeta.getColumnDisplaySize(2));
        assertEquals(6, rsmeta.getColumnDisplaySize(3));
        assertEquals(11, rsmeta.getColumnDisplaySize(4));
        assertEquals(20, rsmeta.getColumnDisplaySize(5));
        assertEquals(10, rsmeta.getColumnDisplaySize(6));
        assertEquals(12, rsmeta.getColumnDisplaySize(7));
        assertEquals(10, rsmeta.getColumnDisplaySize(8));
        assertEquals(10, rsmeta.getColumnDisplaySize(9));
        assertEquals(10, rsmeta.getColumnDisplaySize(10));
        assertEquals(Integer.MAX_VALUE, rsmeta.getColumnDisplaySize(11));
        assertEquals(Integer.MAX_VALUE, rsmeta.getColumnDisplaySize(12));
        assertEquals(10, rsmeta.getColumnDisplaySize(13));
        assertEquals(23, rsmeta.getColumnDisplaySize(14));
    }

    /**
     * To get column size driver does additional request to server.
     * That request should be done before close statement.
     * Otherwise when we call ResultSetMetaData#getColumnDisplaySize() server can be blocked with another statement.
     */
    @Test
    public void columnDisplaySizeDoesNotBlockServerTest() throws ConnException, SQLException {
        String createSql = "create or replace table test_display_size " +
                "(col1 bool, col2 tinyint, col3 smallint, col4 int, col5 bigint, col6 real, col7 double, " +
                "col8 varchar(10), col9 nvarchar(10), col10 text(10), col11 nvarchar, col12 text, col13 date, col14 datetime)";
        String selectSql = "select * from test_display_size";
        int expectedColumnCount = 14;

        ResultSetMetaData rsmeta;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
            ResultSet rs = stmt.executeQuery(selectSql);
            rsmeta = rs.getMetaData();
            rs.close();
        }

        Connector connectorMock = Mockito.mock(Connector.class);
        PowerMockito.mockStatic(ConnectorFactory.class);
        PowerMockito.when(ConnectorFactory.initConnector(IP, PORT, CLUSTER, SSL)).thenReturn(connectorMock);
        SQSocket socketMock = Mockito.mock(SQSocket.class);
        PowerMockito.mockStatic(SQSocket.class);
        PowerMockito.when(SQSocket.connect(IP, PORT, SSL)).thenReturn(socketMock);

        assertEquals(expectedColumnCount, rsmeta.getColumnCount());
        for (int i = 0; i < rsmeta.getColumnCount(); i++) {
            rsmeta.getColumnDisplaySize(i + 1);
        }

        Mockito.verify(connectorMock, VerificationModeFactory.times(0)).connect(any(), any(), any(), any());
        Mockito.verify(socketMock, VerificationModeFactory.times(0)).read(any());
    }
}