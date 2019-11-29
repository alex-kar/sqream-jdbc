package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnectorImpl;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class SQConnectionFactory {

    Connection init(ConnectorImpl client) throws IOException {
        return new SQConnection(client);
    }

    Connection init(Properties connectionInfo) throws NoSuchAlgorithmException, IOException, ScriptException,
            SQLException, KeyManagementException, ConnectorImpl.ConnException {
        return new SQConnection(connectionInfo);
    }
}
