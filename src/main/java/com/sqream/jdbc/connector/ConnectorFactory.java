package com.sqream.jdbc.connector;

import com.sqream.jdbc.SQConnection;
import com.sqream.jdbc.connector.ConnectorImpl;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class ConnectorFactory {

    public Connector getInstance(SQConnection conn) throws KeyManagementException, ScriptException, NoSuchAlgorithmException, ConnectorImpl.ConnException, IOException {
        return new ConnectorImpl(conn.getParams().getIp(), conn.getParams().getPort(), conn.getParams().getCluster(), conn.getParams().getUseSsl());
    }


}
