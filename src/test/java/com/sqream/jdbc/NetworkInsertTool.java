package com.sqream.jdbc;

import com.opencsv.CSVReader;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Types;
import java.util.ArrayList;

public class NetworkInsertTool {

    private static final Integer MAX_BATCH_SIZE = 1000000;

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException{
        JDBCArgs arguments = new JDBCArgs(args);
        run(arguments);

    }

    private static void run(JDBCArgs arguments) throws SQLException, ClassNotFoundException, IOException {
        Connection connection = DriverManager.getConnection(arguments.getConnectionURL(), arguments.user, arguments.password);
        Reader reader = Files.newBufferedReader(arguments.csvPath);
        ResultSet columns = connection.getMetaData().getColumns(null, null, arguments.table, "%");
        int numberOfColumns = 0;
        ArrayList<Integer> columnTypes = new ArrayList<>();
        while(columns.next()){
            numberOfColumns++;
            columnTypes.add(columns.getInt("DATA_TYPE"));
        }

        PreparedStatement ps = connection.prepareStatement(buildInsertStatementBase(arguments, numberOfColumns));

        CSVReader csvReader = new CSVReader(reader);
        String[] nextRecord;
        int statementsInBatch = 0;
        while ((nextRecord = csvReader.readNext()) != null) {
            for (int i=0; i < numberOfColumns; i++) {
                preparedStatementSetColumnEntry(ps, i + 1, nextRecord[i], columnTypes.get(i));
            }
            if(++statementsInBatch == MAX_BATCH_SIZE){
                ps.executeBatch();
                statementsInBatch = 0;
            }
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
    }

    private static String buildInsertStatementBase(JDBCArgs arguments, int numberOfColumns){
        String statement_base = "insert into " + arguments.table + " values (";
        for(int i = 0; i < numberOfColumns; i++){
            if(i < numberOfColumns - 1){
                statement_base += "?, ";
            } else{
                statement_base += "?";
            }
        }
        statement_base += ")";
        return statement_base;
    }

    private static void preparedStatementSetColumnEntry(PreparedStatement ps, int entryIndex, String entry, int columnType) throws SQLException{
        switch(columnType){
            case Types.BOOLEAN: ps.setBoolean(entryIndex, Boolean.parseBoolean(entry)); break;
            case Types.INTEGER: ps.setInt(entryIndex, Integer.parseInt(entry)); break;
            case Types.BIGINT: ps.setLong(entryIndex, Integer.parseInt(entry)); break;
            case Types.FLOAT: ps.setFloat(entryIndex, Float.parseFloat(entry)); break;
            case Types.DOUBLE: ps.setDouble(entryIndex, Double.parseDouble(entry)); break;
            case Types.DATE: ps.setDate(entryIndex, Date.valueOf(entry)); break;
            case Types.TIMESTAMP: ps.setTimestamp(entryIndex, Timestamp.valueOf(entry)); break;
            case Types.VARCHAR:
            case Types.NVARCHAR: ps.setString(entryIndex, entry); break;
            default: throw new IllegalArgumentException("Invalid entry type for " + entry);
        }

    }

    private static class JDBCArgs {

        String ip;
        String port;
        String database;
        String table;
        String user;
        String password;
        String service;
        Boolean cluster;
        Boolean ssl;
        Path csvPath;

        private static final String IP_OPT = "i";
        private static final String IP_OPT_LONG = "ip";

        private static final String PORT_OPT = "port";
        private static final String PORT_OPT_LONG = "port";

        private static final String DBNAME_OPT = "d";
        private static final String DBNAME_OPT_LONG = "database";

        private static final String TABLENAME_OPT = "t";
        private static final String TABLENAME_OPT_LONG = "table";

        private static final String USER_OPT = "u";
        private static final String USER_OPT_LONG = "user";

        private static final String PASSWORD_OPT = "pw";
        private static final String PASSWORD_OPT_LONG = "pass";

        private static final String SERVICE_OPT = "s";
        private static final String SERVICE_OPT_LONG = "service";

        private static final String CLUSTER_OPT = "c";
        private static final String CLUSTER_OPT_LONG = "cluster";

        private static final String SSL_OPT = "ssl";
        private static final String SSL_OPT_LONG = "ssl";

        private static final String CSVPATH_OPT = "csv";
        private static final String CSVPATH_OPT_LONG = "csvpath";

        JDBCArgs(String[] args) {
            parse(args);
        }

        void parse(String[] args){
            Options options = getJDBCOptions();
            CommandLine cmd;

            try{
                CommandLineParser parser = new DefaultParser();
                cmd = parser.parse(options, args);

                ip = cmd.getOptionValue(IP_OPT);
                port = cmd.getOptionValue(PORT_OPT);
                database = cmd.getOptionValue(DBNAME_OPT);
                table = cmd.getOptionValue(TABLENAME_OPT);
                user = cmd.getOptionValue(USER_OPT);
                password = cmd.getOptionValue(PASSWORD_OPT);
                service = cmd.getOptionValue(SERVICE_OPT);
                cluster = cmd.hasOption(CLUSTER_OPT);
                ssl = cmd.hasOption(SSL_OPT);
                csvPath = Paths.get(cmd.getOptionValue(CSVPATH_OPT));

            } catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Network Insert Tool", options);

                System.exit(1);
            }
        }

        String getConnectionURL(){
            StringBuilder stringBuilder = new StringBuilder("jdbc:Sqream://");
            stringBuilder.append(ip);
            stringBuilder.append(":").append(port).append("/");
            stringBuilder.append(database);
            stringBuilder.append(";user=").append(user);
            stringBuilder.append(";password=").append(password);
            if(service != null){
                stringBuilder.append(";service").append(service);
            }
            if(cluster){
                stringBuilder.append(";cluster=true");
            } else{
                stringBuilder.append(";cluster=false");
            }
            if(ssl){
                stringBuilder.append(";ssl=true");
            }else{
                stringBuilder.append(";ssl=false");
            }

            return stringBuilder.toString();
        }

        private void addOption(String shortName, String longName, boolean hasArg, String description, boolean isRequired, Options options){
            Option option = new Option(shortName, longName, hasArg, description);
            option.setRequired(isRequired);
            options.addOption(option);
        }

        private Options getJDBCOptions(){
            Options options = new Options();

            addOption(IP_OPT, IP_OPT_LONG, true, "sqreamd ip address", true, options);

            addOption(PORT_OPT, PORT_OPT_LONG, true, "sqreamd port", true, options);

            addOption(DBNAME_OPT, DBNAME_OPT_LONG, true, "sqream database name", true, options);

            addOption(TABLENAME_OPT, TABLENAME_OPT_LONG, true, "sqream table name", true, options);

            addOption(USER_OPT, USER_OPT_LONG, true, "sqream user", true, options);

            addOption(PASSWORD_OPT, PASSWORD_OPT_LONG, true, "sqream password", true, options);

            addOption(SERVICE_OPT, SERVICE_OPT_LONG, true, "sqream service", false, options);

            addOption(CLUSTER_OPT, CLUSTER_OPT_LONG, false, "is running on a cluster?", false, options);

            addOption(SSL_OPT, SSL_OPT_LONG, false, "enable ssl connection?", false, options);

            addOption(CSVPATH_OPT, CSVPATH_OPT_LONG, true, "table data csv path", true, options);

            return options;
        }

    }

}
