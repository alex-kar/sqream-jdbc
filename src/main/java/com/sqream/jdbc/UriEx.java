package com.sqream.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

// Extended uri data
public class UriEx {
    private static final Logger LOGGER = Logger.getLogger(UriEx.class.getName());

    private URI uri;
    private String provider;
    private String host;
    private int port;
    private String dbName;
    private String user;
    private String pswd;
    private String debug;
    private String logger;
    private String showFullStackTrace;
    private boolean cluster = false;;
    private String service;
    private boolean ssl = false;
    private String schema;
    private boolean skipPicker = false;

    UriEx(String url) throws SQLException {
        parse(url);
        validate(url);
        setDefaultValues();
    }

    private void parse(String url) {
        try {
            this.uri = parseURI(url);
            this.port = uri.getPort();
            this.host = uri.getHost();
            this.provider = uri.getScheme();
            String[] UrlElements = uri.getPath().split(";");
            this.dbName = UrlElements[0].substring(1);

            String entryType = "";
            String entryValue = "";
            for (String element : UrlElements) {
                if (element.indexOf("=") > 0) {
                    String[] entry = element.split("=");
                    if (entry.length < 2) {
                        throw new SQLException("Connect string error , bad entry element : " + element);
                    }

                    entryType = entry[0];
                    entryValue = entry[1];
                    if (entryType.toLowerCase().equals("user"))
                        user = entryValue;
                    else if (entryType.toLowerCase().equals("password"))
                        pswd = entryValue;
                    else if (entryType.toLowerCase().equals("logger"))
                        logger = entryValue;
                    else if (entryType.toLowerCase().equals("debug"))
                        debug = entryValue;
                    else if (entryType.toLowerCase().equals("showfullstacktrace"))
                        showFullStackTrace = entryValue;
                    else if (entryType.toLowerCase().equals("cluster"))
                        cluster = entryValue.toLowerCase().equals("true");
                    else if (entryType.toLowerCase().equals("ssl"))
                        ssl = entryValue.toLowerCase().equals("true");
                    else if (entryType.toLowerCase().equals("service"))
                        service = entryValue;
                    else if (entryType.toLowerCase().equals("schema"))
                        schema = entryType;
                    else if (entryType.toLowerCase().equals("skipPicker"))
                        skipPicker = entryValue.toLowerCase().equals("true");
                    else {
                        throw new IllegalArgumentException(String.format("Unsupported url argument: %s", entryType));
                    }
                }
            }
        } catch (URISyntaxException | SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void validate(String url) throws SQLException {
        String prfx = "jdbc:Sqream";

        if (!url.trim().substring(0, prfx.length()).equals(prfx)) {
            throw new SQLException("Wrong prefix for connection string. Should be jdbc:Sqream but got: " + url.trim().substring(0, prfx.length()));
        }
        if (!provider.toLowerCase().equals("sqream")) {
            throw new SQLException("Bad provider in connection string. Should be sqream but got: " + provider.toLowerCase());
        }
        if (user == null || pswd == null) {
            throw new SQLException("please apply user and password");
        }
        if (dbName == null || "".equals(dbName)) {
            throw new SQLException("connection string : missing database name error");
        }
        if (host == null) {
            throw new SQLException("connection string : missing host ip error");
        }
        if (port == -1) {
            throw new SQLException("connection string : missing port error");
        }
    }

    private void setDefaultValues() {
        if(service == null) {
            log("no service passed, defaulting to sqream");
            service = "sqream";
        }
        if(schema == null) {
            log("no schema passed, defaulting to public");
            schema = "public";
        }
    }

    public URI getUri() {
        return uri;
    }

    public String getProvider() {
        return provider;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }

    public String getUser() {
        return user;
    }

    public String getPswd() {
        return pswd;
    }

    public String getDebug() {
        return debug;
    }

    public String getLogger() {
        return logger;
    }

    public String getShowFullStackTrace() {
        return showFullStackTrace;
    }

    public boolean getCluster() {
        return cluster;
    }

    public String getService() {
        return service;
    }

    public boolean getSsl() {
        return ssl;
    }

    public String getSchema() {
        return schema;
    }

    private URI parseURI(String url) throws SQLException, URISyntaxException {
        if (url == null || url.length() < 6) {
            throw new SQLException("Connect string general error : " + url
                    + "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=sa'");
        }
        URI result = new URI(url.substring(5)); // cause first 5 chars are not relevant
        if (result.getPath() == null) {
            throw new SQLException("Connect string general error : " + url
                    + "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=sa'");
        }
        return result;
    }

    private void log(String str) {
        LOGGER.log(Level.FINE, str);
    }
}
