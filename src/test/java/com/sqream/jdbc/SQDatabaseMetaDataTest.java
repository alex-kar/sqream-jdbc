package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQDatabaseMetaDataTest {
    private static SQDatabaseMetaData metadata;

    @Before
    public void setUp() throws ConnException {

        Connector connector = new ConnectorImpl(IP, PORT, CLUSTER, SSL);
        SQConnection connection = new SQConnection(connector);
        metadata = new SQDatabaseMetaData(connector, connection, USER, DATABASE);
    }

    @Test
    public void storesLowerCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.storesLowerCaseIdentifiers());
    }

    @Test
    public void storesLowerCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesLowerCaseQuotedIdentifiers());
    }

    @Test
    public void storesUpperCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesUpperCaseIdentifiers());
    }

    @Test
    public void storesUpperCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesUpperCaseQuotedIdentifiers());
    }

    @Test
    public void storesMixedCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesMixedCaseIdentifiers());
    }

    @Test
    public void storesMixedCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.storesMixedCaseQuotedIdentifiers());
    }

    @Test
    public void supportsMixedCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.supportsMixedCaseIdentifiers());
    }

    @Test
    public void supportsMixedCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.supportsMixedCaseQuotedIdentifiers());
    }

    @Test
    public void supportsBatchUpdatesTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.supportsBatchUpdates());
    }

    @Test
    public void supportsCatalogsInDataManipulationTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.supportsCatalogsInDataManipulation());
    }

    @Test
    public void supportsSchemasInDataManipulationTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.supportsSchemasInDataManipulation());
    }

    @Test
    public void whenCatalogStarGetAllColumnsTest() throws SQLException {
        String CATALOG = "*";
        Set<String> columnsByQuery = new HashSet<>();
        Set<String> columnsFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_columns('%s', '*', '*', '*');", CATALOG));
                while (rs.next()) {
                    columnsByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(CATALOG, null, null, null);
            while (rs.next()) {
                columnsFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(columnsByQuery.size(), columnsFromMetadata.size());
        for (String tableFromQuery : columnsByQuery) {
            assertTrue(columnsFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void whenCatalogNullGetCurrentDatabaseColumnsTest() throws SQLException {
        Set<String> columnsByQuery = new HashSet<>();
        Set<String> columnsFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_columns('%s', '*', '*', '*');", DATABASE));
                while (rs.next()) {
                    columnsByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(null, null, null, null);
            while (rs.next()) {
                columnsFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(columnsByQuery.size(), columnsFromMetadata.size());
        for (String tableFromQuery : columnsByQuery) {
            assertTrue(columnsFromMetadata.contains(tableFromQuery));
        }
    }
}
