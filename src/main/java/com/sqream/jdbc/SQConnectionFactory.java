package com.sqream.jdbc;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class SQConnectionFactory {

    Connection initConnection(Connector client) throws IOException {
        return new SQConnection(client);
    }

    Connection initConnection(Properties connectionInfo) throws NoSuchAlgorithmException, IOException, ScriptException,
            SQLException, KeyManagementException, Connector.ConnException {
        return new SQConnection(connectionInfo);
    }
}
