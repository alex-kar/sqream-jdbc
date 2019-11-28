package com.sqream.jdbc;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

class ConnectorFactory {

    Connector getInstance(SQConnection conn) throws KeyManagementException, ScriptException, NoSuchAlgorithmException, Connector.ConnException, IOException {
        return new Connector(conn.getParams().getIp(), conn.getParams().getPort(), conn.getParams().getCluster(), conn.getParams().getUseSsl());
    }
}
