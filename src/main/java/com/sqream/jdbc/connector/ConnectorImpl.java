package com.sqream.jdbc.connector;

//Packing and unpacking columnar data
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// Socket communication

// More SSL shite

// JSON parsing library
import com.eclipsesource.json.ParseException;
import com.eclipsesource.json.WriterConfig;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonArray;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;
import java.util.List;
import java.text.MessageFormat;

// Datatypes for building columns and other
import java.util.BitSet;

// Unicode related
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

// Date / Time related
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
// Aux
import java.util.Arrays;   //  To allow debug prints via Arrays.toString
import java.util.stream.IntStream;

//Exceptions
import javax.script.ScriptException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

// SSL over SocketChannel abstraction

import static com.sqream.jdbc.utils.Utils.*;
import static com.sqream.jdbc.utils.Utils.formJson;

public class ConnectorImpl implements Connector {

    SQSocketConnector s;

    // Class variables
    // ---------------
        
    // Protocol related

    int connection_id = -1;
    int statementId = -1;
    String varchar_encoding = "ascii";  // default encoding/decoding for varchar columns
    static Charset UTF8 = StandardCharsets.UTF_8;
    boolean charitable = false;

    String ip;
    int port;
    String database;
    String user = "sqream";
    String password = "sqream";
    String service = "sqream";
    boolean useSsl;
    
    // Reconnecting parameters that don't appear before that stage
    int listener_id;
    int port_ssl;
    boolean reconnect;

    String json_wrapper = "Java.asJSONCompatible({0})";
    //@SuppressWarnings("rawtypes") // Remove "Map is a raw type"  warning
    //https://stackoverflow.com/questions/2770321/what-is-a-raw-type-and-why-shouldnt-we-use-it
    JsonObject col_data; // response_json
    JsonObject response_json;
    JsonArray query_type; // JSONListAdapter represents a list inside a JSON
    JsonArray fetch_sizes;
    JsonArray col_type_data; 
    Map<String, String> prepare_map;
    
    // Message sending related
    ByteBuffer response_buffer = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
    int message_length, bytes_read, total_bytes_read;
    String response_string;
    boolean fetch_msg = false;
    
    		
    // Binary data related

    int FLUSH_SIZE = 10 * (int) Math.pow(10, 6);
    //byte [] buffer = new byte[FLUSH_SIZE];
    int row_size, rows_per_flush;
    int max_string_len = 0;   // For preallocating a bytearray for getVarchar/Nvarchar
    int fetch_size = 0;      // How much to retrieve on a select statement
    
    // Column metadata
    String statement_type;
    int row_length;
    String [] col_names;
    HashMap<String, Integer> col_names_map;
    String [] col_types;
    int []    col_sizes;
    BitSet col_nullable;
    BitSet col_tvc;

    boolean openStatement = false;
    int chunk_size;
    boolean closed_by_prefetch;
    
    // Column Storage
    List<ByteBuffer[]> data_buffers = new ArrayList<>();
    List<ByteBuffer[]> null_buffers = new ArrayList<>();
    List<ByteBuffer []> nvarc_len_buffers = new ArrayList<>();
    List<Integer> rows_per_batch = new ArrayList<>();
    ByteBuffer[] data_columns;
    int rows_in_current_batch;
    //byte[][] null_columns;
    ByteBuffer[] null_columns;
    ByteBuffer null_resetter;
    ByteBuffer[] nvarc_len_columns;
    ByteBuffer [] null_balls;
    int fetch_limit = 0;
    
    // Get / Set related
    int row_counter, total_row_counter;
    BitSet columns_set;
    int total_bytes;
    boolean is_null;

    byte[] string_bytes; // Storing converted string to be set
    ByteBuffer[] fetch_buffers;
    int new_rows_fetched, total_rows_fetched;
    byte [] spaces; 
    int nvarc_len;
    int col_num;
    int[] col_calls;
    
    // Date/Time conversion related
    static ZoneId UTC = ZoneOffset.UTC;
    static ZoneId system_tz = ZoneId.systemDefault();
    static int year, month, day, hour, minutes, seconds, ms;
    static int date_as_int, time_as_int;
    static long dt_as_long;
    static LocalDate local_date;
    static LocalDateTime local_datetime;

    // Managing stop_statement
    private AtomicBoolean IsCancelStatement = new AtomicBoolean(false);
    
    
    // Communication Strings
    // ---------------------

    String connectDatabase = "'{'\"connectDatabase\":\"{0}\", \"username\":\"{1}\", \"password\":\"{2}\", \"service\":\"{3}\"'}'";
    String prepareStatement = "'{'\"prepareStatement\":\"{0}\", \"chunkSize\":{1}'}'";
    String reconnectDatabase = "'{'\"reconnectDatabase\":\"{0}\", \"username\":\"{1}\", \"password\":\"{2}\", \"service\":\"{3}\", \"connectionId\":{4, number, #}, \"listenerId\":{5, number, #}'}'";
    String reconstructStatement = "'{'\"reconstructStatement\":{0, number, #}'}'";
    String put = "'{'\"put\":{0, number, #}'}'";
    
    // Aux Classes
    // -----------
    
    public static class ConnException extends Exception {
        /*  Connector exception class */
        
        private static final long serialVersionUID = 1L;
        public ConnException(String message) {
            super(message);
        }
    }

    // Constructor  
    // -----------

    public ConnectorImpl(String _ip, int _port, boolean _cluster, boolean _ssl) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        /* JSON parsing engine setup, initial socket connection */

        port = _port;
        ip = _ip;
        useSsl = _ssl;
        s = new SQSocketConnector(ip, port);
        s.connect(useSsl);

        // Clustered connection - reconnect to actual ip and port
        if (_cluster) {
            // Get data from server picker
            response_buffer.clear();
            //_read_data(response_buffer, 0); // IP address size may vary
            bytes_read = s.read(response_buffer);
            response_buffer.flip();     
		    if (bytes_read == -1) {
		    	throw new IOException("Socket closed When trying to connect to server picker");
            } 

            // Read size of IP address (7-15 bytes) and get the IP
            byte [] ip_bytes = new byte[response_buffer.getInt()]; // Retreiving ip from clustered connection
            response_buffer.get(ip_bytes);
            ip = new String(ip_bytes, UTF8);

            // Last is the port
            port = response_buffer.getInt();

            s.reconnect(ip, port, useSsl);
        }
    }
    
    
    // Internal Mechanism Functions
    // ----------------------------
    /*  
     * (1) _parse_sqream_json -        Return Map from a JSON string
     * (2) _generate_headered_buffer - Create ByteBuffer for message and fill the header
     * (3)  _parse_response_header -   Extract header info from received ByteBuffer 
     * (4)  _send_data -               
     * (5) _send_message -            
     */
    
    // (1) 
    //@SuppressWarnings("rawtypes")  // Remove "Map is a raw type" warning
    /*
    Map<String,Object> _parse_sqream_json(String json) throws ScriptException, ConnException { 
    	
    	String error;
    	
    	response_json = (Map<String, Object>) engine.eval(MessageFormat.format(json_wrapper, json));
        if (response_json.containsKey("error")) {
            error = (String)response_json.get("error");
            
            if (!error.contains("stop_statement could not find a statement"))
        		throw new ConnException("Error from SQream:" + error);
        }
    	
    	return response_json;
    }
    //*/
    
    JsonObject _parse_sqream_json(String json_str) {
    	
    	
    	return Json.parse(json_str).asObject();
    	
    }
    
    Boolean _validate_open(String statement_type) throws ConnException {
    	
    	if (!isOpen()) {
    		throw new ConnException("Trying to run command " + statement_type + " but connection closed");
    	}
    	
		if (!openStatement) {
			throw new ConnException("Trying to run command " + statement_type + " but statement closed");
		}
    	
    	return true;
    }
    
    String _validate_response(String response, String expected) throws ConnException {
        
        if (!response.equals(expected))  // !response.contains("stop_statement could not find a statement")
            throw new ConnException("Expected message: " + expected + " but got " + response);
        
        return response;
    }
    
    
    // Internal API Functions
    // ----------------------------
    /*  
     * (1) _parse_query_type() - 
     * (2) _flush()
     */
    
    // ()  /* Unpack the json of column data arriving via queryType/(named). Called by prepare()  */
    //@SuppressWarnings("rawtypes") // "Map is a raw type" @ col_data = (Map)query_type.get(idx);
    void _parse_query_type(JsonArray query_type) throws IOException, ScriptException{
        
        row_length = query_type.size();
        if(row_length ==0)
            return;
        
        // Set metadata arrays given the amount of columns
        col_names = new String[row_length];
        col_types = new String[row_length];
        col_sizes = new int[row_length];
        col_nullable = new BitSet(row_length);
        col_tvc = new BitSet(row_length);
        col_names_map = new HashMap<String, Integer>();
        
        col_calls = new int[row_length];
        // Parse the queryType json to get metadata for every column
        // An internal item looks like: {"isTrueVarChar":false,"nullable":true,"type":["ftInt",4,0]}
        for(int idx=0; idx < row_length; idx++) {
            // Parse JSON to correct objects
            col_data = query_type.get(idx).asObject();
            col_type_data = col_data.get("type").asArray(); // type is a list of 3 items
            
            // Assign data from parsed JSON objects to metadata arrays
            col_nullable.set(idx, col_data.get("nullable").asBoolean()); 
            col_tvc.set(idx, col_data.get("isTrueVarChar").asBoolean()); 
            col_names[idx] = statement_type.equals("SELECT") ? col_data.get("name").asString(): "denied";
            col_names_map.put(col_names[idx].toLowerCase(), idx +1);
            col_types[idx] = col_type_data.get(0).asString();
            col_sizes[idx] = col_type_data.get(1).asInt();
        }
        
        // Create Storage for insert / select operations
        if (statement_type.equals("INSERT")) {
            // Calculate number of rows to flush at
            row_size = IntStream.of(col_sizes).sum() + col_nullable.cardinality();    // not calculating nvarc lengths for now
            rows_per_flush = FLUSH_SIZE / row_size;
            // rows_per_flush = 500000;
            // Buffer arrays for column storage
            data_columns = new ByteBuffer[row_length];
            null_columns = new ByteBuffer[row_length];
            null_resetter = ByteBuffer.allocate(rows_per_flush);
            nvarc_len_columns = new ByteBuffer[row_length];
            
            // Instantiate flags for managing network insert operations
            row_counter = 0;
            columns_set = new BitSet(row_length); // defaults to false
            
            // Initiate buffers for each column using the metadata
            for (int idx=0; idx < row_length; idx++) {
                data_columns[idx] = ByteBuffer.allocateDirect(col_sizes[idx]*rows_per_flush).order(ByteOrder.LITTLE_ENDIAN);
                null_columns[idx] = col_nullable.get(idx) ? ByteBuffer.allocateDirect(rows_per_flush).order(ByteOrder.LITTLE_ENDIAN) : null;
                nvarc_len_columns[idx] = col_tvc.get(idx) ? ByteBuffer.allocateDirect(4*rows_per_flush).order(ByteOrder.LITTLE_ENDIAN) : null;
            }
        }
        if (statement_type.equals("SELECT")) { 
            
            // Instantiate select counters, Initial storage same as insert
            row_counter = -1;
            total_row_counter = 0;
            total_rows_fetched = -1;     
            
            // Get the maximal string size (or size fo another type if strings are very small)
            string_bytes = new byte[Arrays.stream(col_sizes).max().getAsInt()];
        }
    }
    
    int _fetch() throws IOException, ScriptException, ConnException {
        /* Request and get data from SQream following a SELECT query */
        
        // Send fetch request and get metadata on data to be received
        response_json = _parse_sqream_json(s.sendMessage(formJson("fetch"), true));
        new_rows_fetched = response_json.get("rows").asInt();
        fetch_sizes =   response_json.get("colSzs").asArray();  // Chronological sizes of all rows recieved, only needed for nvarchars
        if (new_rows_fetched == 0) {
        	close();  // Auto closing statement if done fetching
        	return new_rows_fetched;
        }
        // Initiate storage columns using the "colSzs" returned by SQream
        // All buffers in a single array to use SocketChannel's read(ByteBuffer[] dsts)
        int col_buf_size;
        fetch_buffers = new ByteBuffer[fetch_sizes.size()];
        data_columns = new ByteBuffer[row_length];
        null_columns = new ByteBuffer[row_length];
        nvarc_len_columns = new ByteBuffer[row_length];
        
    	for (int idx=0; idx < fetch_sizes.size(); idx++) 
    		fetch_buffers[idx] = ByteBuffer.allocateDirect(fetch_sizes.get(idx).asInt()).order(ByteOrder.LITTLE_ENDIAN);        
        
        // Sort buffers to appropriate arrays (row_length determied during _query_type())
        for (int idx=0, buf_idx = 0; idx < row_length; idx++, buf_idx++) {  
            if(col_nullable.get(idx)) {
                null_columns[idx] = fetch_buffers[buf_idx];
                //fetch_buffers[buf_idx].get(null_columns[idx]);
                //fetch_buffers[buf_idx].clear();
                //null_balls[idx] = fetch_buffers[buf_idx];
                buf_idx++;
            } else
                null_columns[idx] = null;
            
            if(col_tvc.get(idx)) {
                nvarc_len_columns[idx] = fetch_buffers[buf_idx];
                buf_idx++;
            } else
                nvarc_len_columns[idx] = null;
            
            data_columns[idx] = fetch_buffers[buf_idx];
        }
        
        // Add buffers to buffer list
        data_buffers.add(data_columns);
        null_buffers.add(null_columns);
        nvarc_len_buffers.add(nvarc_len_columns);
        rows_per_batch.add(new_rows_fetched);
        
        // Initial naive implememntation - Get all socket data in advance
        bytes_read = s.getParseHeader();   // Get header out of the way
        for (ByteBuffer fetched : fetch_buffers) {
            s.readData(fetched, fetched.capacity());
        	//Arrays.stream(fetch_buffers).forEach(fetched -> fetched.flip());
        }
 
        return new_rows_fetched;  // counter nullified by next()
    }
    
    
    int _fetch(int row_amount) throws IOException, ScriptException, ConnException {
    	int total_fetched = 0;
    	int new_rows_fetched;
    	
    	if (row_amount < -1) {
    		throw new ConnException("row_amount should be positive, got " + row_amount);
    	}
    	if (row_amount == -1) {
    		// Place for adding logic for previos fetching behavior - per
    		// requirement fetch
    	}
    	else {  // positive row amount
    		while (row_amount == 0 || total_fetched < row_amount) {
    			new_rows_fetched = _fetch();
    			if (new_rows_fetched ==0) 
    				break;
    			total_fetched += new_rows_fetched;
    		}
    	}
    	close(); 
    	rows_in_current_batch = 0;
    	
    	
    	return total_fetched;
    }
    
    
    
    int _flush(int row_counter) throws IOException, ConnException {
        /* Send columnar data buffers to SQream. Called by next() and close() */
        
        if (!statement_type.equals("INSERT") || row_counter == 0) {  // Not an insert statement
            return 0;
        }

        // Send put message
        s.sendMessage(MessageFormat.format(put, row_counter), false);
        
        // Get total column length for the header
        total_bytes = 0;
        for(int idx=0; idx < row_length; idx++) {
            total_bytes += (null_columns[idx] != null) ? row_counter : 0;
            total_bytes += (nvarc_len_columns[idx] != null) ? 4 * row_counter : 0;

            total_bytes += data_columns[idx].position();
        }
        
        // Send header with total binary insert
        ByteBuffer header_buffer = s.generateHeaderedBuffer(total_bytes, false);
        s.sendData(header_buffer, false);

        // Send available columns
        for(int idx=0; idx < row_length; idx++) {
            if(null_columns[idx] != null) {
                s.sendData((ByteBuffer)null_columns[idx].position(row_counter), false);
            }
            if(nvarc_len_columns[idx] != null) {
                s.sendData(nvarc_len_columns[idx], false);
            }
            s.sendData(data_columns[idx], false);
        }
        
        _validate_response(s.sendData(null, true), formJson("putted"));  // Get {"putted" : "putted"}
        
        
        return row_counter;  // counter nullified by next()
    }
    
    
    // User API Functions
    /* ------------------
     * connect(), execute(), next(), close(), close_conenction() 
     * 
     */
    @Override
    public int connect(String _database, String _user, String _password, String _service) throws IOException, ScriptException, ConnException {
        //"'{'\"username\":\"{0}\", \"password\":\"{1}\", \"connectDatabase\":\"{2}\", \"service\":\"{3}\"'}'";
        
        database = _database;
        user = _user;
        password = _password;
        service = _service;
        
        String connStr = MessageFormat.format(connectDatabase, database, user, password, service);
        response_json = _parse_sqream_json(s.sendMessage(connStr, true));
        connection_id = response_json.get("connectionId").asInt(); 
        varchar_encoding = response_json.getString("varcharEncoding", "ascii");
    	varchar_encoding = (varchar_encoding.contains("874"))? "cp874" : "ascii";
        
        return connection_id;
    }
    
    @Override
    public int execute(String statement) throws IOException, ScriptException, ConnException, KeyManagementException, NoSuchAlgorithmException {
    	/* Retains behavior of original execute()  */
    	
    	int default_chunksize = (int) Math.pow(10,6);
    	return execute(statement, default_chunksize);	
    }

    @Override
    public int execute(String statement, int _chunk_size) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException {
        
    	chunk_size = _chunk_size;
    	if (chunk_size < 0)
    		throw new ConnException("chunk_size should be positive, got " + chunk_size);
    	
    	/* getStatementId, prepareStatement, reconnect, execute, queryType  */
        charitable = true;
    	if (openStatement)
    		if (charitable)  // Automatically close previous unclosed statement
    			close();
    		else
    			throw new ConnException("Trying to run a statement when another was not closed. Open statement id: " + statementId + " on connection: " + connection_id);
    	openStatement = true;
        // Get statement ID, send prepareStatement and get response parameters
        statementId = _parse_sqream_json(s.sendMessage(formJson("getStatementId"), true)).get("statementId").asInt();
        
        // Generating a valid json string via external library
        JsonObject prepare_jsonify;
        try
        {
	        prepare_jsonify = Json.object()
		    		.add("prepareStatement", statement)
		    		.add("chunkSize", chunk_size);  
        }
    	catch(ParseException e)
        {
    		throw new ConnException ("Could not parse the statement for PrepareStatement");
        }
        
        // Jsonifying via standard library - verify with test
        //engine_bindings.put("statement", statement);
        //String prepareStr = (String) engine.eval("JSON.stringify({prepareStatement: statement, chunkSize: 0})");
        
        String prepareStr = prepare_jsonify.toString(WriterConfig.MINIMAL);
        
        response_json =  _parse_sqream_json(s.sendMessage(prepareStr, true));
        
        // Parse response parameters
        listener_id =    response_json.get("listener_id").asInt();
        port =           response_json.get("port").asInt();
        port_ssl =       response_json.get("port_ssl").asInt();
        reconnect =      response_json.get("reconnect").asBoolean();
        ip =             response_json.get("ip").asString();
        
        port = useSsl ? port_ssl : port;
        // Reconnect and reestablish statement if redirected by load balancer
        if (reconnect) {
            s.close();
            
            s.reconnect(ip, port, useSsl);
            
            // Sending reconnect, reconstruct commands
            String reconnectStr = MessageFormat.format(reconnectDatabase, database, user, password, service, connection_id, listener_id);
            s.sendMessage(reconnectStr, true);
            _validate_response(s.sendMessage( MessageFormat.format(reconstructStatement, statementId), true), formJson("statementReconstructed"));

        }  
         
        // Getting query type manouver and setting the type of query
        _validate_response(s.sendMessage(formJson("execute"), true), formJson("executed"));
        query_type =  _parse_sqream_json(s.sendMessage(formJson("queryTypeIn"), true)).get("queryType").asArray();
        
        if (query_type.isEmpty()) {
            query_type =  _parse_sqream_json(s.sendMessage(formJson("queryTypeOut"), true)).get("queryTypeNamed").asArray();
            statement_type = query_type.isEmpty() ? "DML" : "SELECT";
        }
        else {
            statement_type = "INSERT";
        }   
        
        // Select or Insert statement - parse queryType response for metadata
        if (!statement_type.equals("DML"))  
            _parse_query_type(query_type);
        
        // First fetch on the house, auto close statement if no data returned
        if (statement_type.equals("SELECT")) {
        	total_rows_fetched = _fetch(fetch_limit); // 0 - prefetch all data 
             //if (total_rows_fetched < (chunk_size == 0 ? 1 : chunk_size)) {
        }
        
        return statementId;
    }
    
    @Override
    public boolean next() throws ConnException, IOException, ScriptException {
        /* See that all needed buffers were set, flush if needed, nullify relevant
           counters */
        
        if (statement_type.equals("INSERT")) {
                
            // Were all columns set
            //if (!IntStream.range(0, columns_set.length).allMatch(i -> columns_set[i]))
            if (columns_set.cardinality() < row_length)
                throw new ConnException ("All columns must be set before calling next(). Set " + columns_set.cardinality() +  " columns out of "  + row_length);
            
            // Nullify column flags and update counter
            columns_set.clear();  
            row_counter++;
                    
            // Flush and clean if needed
            if (row_counter == rows_per_flush) {
                _flush(row_counter);    
                
                // After flush, clear row counter and all buffers
                row_counter = 0;
                for(int idx=0; idx < row_length; idx++) {
                    if (null_columns[idx] != null) {  
                    	// Clear doesn't actually nullify/reset the data
                        null_columns[idx].clear();
                        null_columns[idx].put(null_resetter);
                        null_columns[idx].clear();
                    }
                    if(nvarc_len_columns[idx] != null) 
                        nvarc_len_columns[idx].clear();
                    data_columns[idx].clear();
                }
            }
        }
        else if (statement_type.equals("SELECT")) {
            //print ("select row counter: " + row_counter + " total: " + total_rows_fetched);
        	Arrays.fill(col_calls, 0); // calls in the same fetch - for varchar / nvarchar
        	if (fetch_limit !=0 && total_row_counter == fetch_limit)
        		return false;  // MaxRow limit reached, stop even if more data was fetched
        	// If all data has been read, try to fetch more
        	if (row_counter == (rows_in_current_batch -1)) {
        		row_counter = -1;
        		if (rows_per_batch.size() == 0) 
                    return false; // No more data and we've read all we have
        		
        		// Set new active buffer to be reading data from
        		rows_in_current_batch = rows_per_batch.get(0);
        		data_columns = data_buffers.get(0);
        		null_columns = null_buffers.get(0);
        		nvarc_len_columns = nvarc_len_buffers.get(0);
        		
        		/*
        		print ("rows in current batch:" + rows_in_current_batch);
        		print ("data columns:" + data_columns[0]);
        		print ("null columns:" + null_columns[0]);
        		print ("nvarc len columns:" + nvarc_len_columns[0]);
        		print ("data size: " + data_buffers.size());
        		// */
        		
        		// Remove active buffer from list
        		data_buffers.remove(0);
                null_buffers.remove(0);
                nvarc_len_buffers.remove(0);
                rows_per_batch.remove(0);
        		
            }
            row_counter++;
            total_row_counter++;
        
        }
        else if (statement_type.equals("DML"))
            throw new ConnException ("Calling next() on a non insert / select query");
        
        else
            throw new ConnException ("Calling next() on a statement type different than INSERT / SELECT / DML: " + statement_type);
            
        return true;
    }
    
    @Override
    public Boolean close() throws IOException, ScriptException, ConnException {

    	String res = "";

    	if (isOpen()) {
    		if (openStatement) {

    			if (statement_type!= null && statement_type.equals("INSERT")) {
    	            _flush(row_counter);
    	        }
    	            // Statement is finished so no need to reset row_counter etc

    			res = _validate_response(s.sendMessage(formJson("closeStatement"), true), formJson("statementClosed"));
    	        openStatement = false;  // set to true in execute()
    		}
    		else
    			return false;  //res =  "statement " + statement_id + " already closed";
    	}
    	else
    		return false;  //res =  "connection already closed";
        
        return true;
    }
    
    @Override
    public boolean closeConnection() throws IOException, ScriptException, ConnException {
        if (isOpen()) {
        	if (openStatement) { // Close open statement if exists
                close();
        	}
        	_validate_response(s.sendMessage(formJson("closeConnection"), true), formJson("connectionClosed"));
	        s.close();
        }
        return true;
    }
    
    boolean _validate_index(int col_num) throws ConnException {
    	if (col_num <0 || col_num > row_length)
    		 throw new ConnException("Illegal index on get/set\nAllowed indices are 0-" + (row_length -1));
    	
    	return true;
    }
    
    // Gets
    // ----
    
    boolean _validate_get(int col_num, String value_type) throws ConnException {
        /* If get function is appropriate, return true for non null values, false for a null */
        // Validate type
        if (!col_types[col_num].equals(value_type))
            throw new ConnException("Trying to get a value of type " + value_type + " from column number " + col_num + " of type " + col_types[col_num]);
        
        // print ("null column holder: " + Arrays.toString(null_columns));
        return null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0;
    }
    
    // -o-o-o-o-o    By index -o-o-o-o-o
    @Override
    public Boolean getBoolean(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        return (_validate_get(col_num, "ftBool")) ? data_columns[col_num].get(row_counter) != 0 : null;
    }
    
    @Override
    public Byte get_ubyte(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        return (_validate_get(col_num, "ftUByte")) ? data_columns[col_num].get(row_counter) : null;
    }  // .get().toUnsignedInt()  -->  to allow values between 127-255 
    
    @Override
    public Short get_short(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	//*
    	if (col_types[col_num].equals("ftUByte"))
            return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (short)(data_columns[col_num].get(row_counter) & 0xFF) : null;
		//*/
		
    	return (_validate_get(col_num, "ftShort")) ? data_columns[col_num].getShort(row_counter * 2) : null;
    }
    
    @Override
    public Integer get_int(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	//*
	    if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (int)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (int)(data_columns[col_num].get(row_counter) & 0xFF) : null;
		//*/
        return (_validate_get(col_num, "ftInt")) ? data_columns[col_num].getInt(row_counter * 4) : null;
        //return (null_balls[col_num].get() == 0) ? data_columns[col_num].getInt() : null;
    }
    
    @Override
    public Long get_long(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	
    	if (col_types[col_num].equals("ftInt"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (long)data_columns[col_num].getInt(row_counter * 4) : null;
    	else if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (long)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (long)(data_columns[col_num].get(row_counter) & 0xFF) : null;
	        
        return (_validate_get(col_num, "ftLong")) ? data_columns[col_num].getLong(row_counter * 8) : null;
    }
    
    @Override
    public Float get_float(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        
    	if (col_types[col_num].equals("ftInt"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (float)data_columns[col_num].getInt(row_counter * 4) : null;
    	else if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (float)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (float)(data_columns[col_num].get(row_counter) & 0xFF) : null;
	        
    	return (_validate_get(col_num, "ftFloat")) ? data_columns[col_num].getFloat(row_counter * 4) : null;
    }
    
    @Override
    public Double get_double(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	
	    if (col_types[col_num].equals("ftFloat"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getFloat(row_counter * 4) : null;
        else if (col_types[col_num].equals("ftLong"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getLong(row_counter * 8) : null;
        else if (col_types[col_num].equals("ftInt"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getInt(row_counter * 4) : null;
    	else if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)(data_columns[col_num].get(row_counter) & 0xFF) : null;
	        
        
        return (_validate_get(col_num, "ftDouble")) ? data_columns[col_num].getDouble(row_counter * 8) : null;
    }
    
    @Override
    public String get_varchar(int col_num) throws ConnException, UnsupportedEncodingException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Get bytes the size of the varchar column into string_bytes
        if (col_calls[col_num]++ > 0) {
	        // Resetting buffer position in case someone runs the same get()
	        data_columns[col_num].position(data_columns[col_num].position() -col_sizes[col_num]);
        }
        data_columns[col_num].get(string_bytes, 0, col_sizes[col_num]);
        
        return (_validate_get(col_num, "ftVarchar")) ? ("X" + (new String(string_bytes, 0, col_sizes[col_num], varchar_encoding))).trim().substring(1) : null;
    }
    
    @Override
    public String get_nvarchar(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        nvarc_len = nvarc_len_columns[col_num].getInt(row_counter * 4);
        
        // Get bytes the size of this specific nvarchar into string_bytes
        if (col_calls[col_num]++ > 0)
        	data_columns[col_num].position(data_columns[col_num].position() - nvarc_len);
        data_columns[col_num].get(string_bytes, 0, nvarc_len);
        
        return (_validate_get(col_num, "ftBlob")) ? new String(string_bytes, 0, nvarc_len, UTF8) : null;
    }
    
    @Override
    public Date get_date(int col_num, ZoneId zone) throws ConnException {   col_num--;  // set / get work with starting index 1
		_validate_index(col_num);
        
		return (_validate_get(col_num, "ftDate")) ? intToDate(data_columns[col_num].getInt(4*row_counter), zone) : null;
    }
    
    @Override
    public Timestamp get_datetime(int col_num, ZoneId zone) throws ConnException {   col_num--;  // set / get work with starting index 1
		_validate_index(col_num);
		return (_validate_get(col_num, "ftDateTime")) ? longToDt(data_columns[col_num].getLong(8* row_counter), zone) : null;
	}

    @Override
    public Date get_date(int col_num) throws ConnException {  
    
    	return get_date(col_num, system_tz); // system_tz, UTC
    }
    
    @Override
    public Timestamp get_datetime(int col_num) throws ConnException {   // set / get work with starting index 1
    	
        return get_datetime(col_num, system_tz); // system_tz, UTC
    }

    // -o-o-o-o-o  By column name -o-o-o-o-o
    @Override
    public Boolean getBoolean(String col_name) throws ConnException {
    	
        return getBoolean(col_names_map.get(col_name));
    }

    @Override
    public Byte get_ubyte(String col_name) throws ConnException {  
    
        return get_ubyte(col_names_map.get(col_name));
    }

    @Override
    public Short get_short(String col_name) throws ConnException {  
        
        return get_short(col_names_map.get(col_name));
    }

    @Override
    public Integer get_int(String col_name) throws ConnException {     
        
        return get_int(col_names_map.get(col_name));
    }

    @Override
    public Long get_long(String col_name) throws ConnException {  
    	
        return get_long(col_names_map.get(col_name));
    }

    @Override
    public Float get_float(String col_name) throws ConnException {   
        
        return get_float(col_names_map.get(col_name));
    }

    @Override
    public Double get_double(String col_name) throws ConnException {  
        
        return get_double(col_names_map.get(col_name));
    }

    @Override
    public String get_varchar(String col_name) throws ConnException, UnsupportedEncodingException {  
        
        return get_varchar(col_names_map.get(col_name));
    }

    @Override
    public String get_nvarchar(String col_name) throws ConnException {   
        
        return get_nvarchar(col_names_map.get(col_name));
    }

    @Override
    public Date get_date(String col_name) throws ConnException {   
            
        return get_date(col_names_map.get(col_name));
    }

    @Override
    public Date get_date(String col_name, ZoneId zone) throws ConnException {   
        
        return get_date(col_names_map.get(col_name), zone);
    }

    @Override
    public Timestamp get_datetime(String col_name) throws ConnException {   
        
        return get_datetime(col_names_map.get(col_name));
    }

    @Override
    public Timestamp get_datetime(String col_name, ZoneId zone) throws ConnException {   
        
        return get_datetime(col_names_map.get(col_name), zone);
    }
    
    // Sets
    // ----
    
    boolean _validate_set(int col_num, Object value, String value_type) throws ConnException {
        
        // Validate type
        if (!col_types[col_num].equals(value_type))
            throw new ConnException("Trying to set " + value_type + " on a column number " + col_num + " of type " + col_types[col_num]);
    
        // Optional null handling - if null is appropriate, mark null column
        if (value == null) {
            if (null_columns[col_num] != null) { 
                null_columns[col_num].put((byte)1);
                is_null = true;
            } else
                throw new ConnException("Trying to set null on a non nullable column of type " + col_types[col_num]);
        }
        else
            is_null = false;
        
        
        return is_null;
    }


    public boolean set_boolean(int col_num, Boolean value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].put((byte)(_validate_set(col_num, value, "ftBool") ? 0 : (value == true) ? 1 : 0));
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_ubyte(int col_num, Byte value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Check the byte is positive
        if (value!= null && value < 0 ) 
                throw new ConnException("Trying to set a negative byte value on an unsigned byte column");
        
        // Set actual value - null or positive at this point
        data_columns[col_num].put(_validate_set(col_num, value, "ftUByte") ? 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
    
     
    public boolean set_short(int col_num, Short value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putShort(_validate_set(col_num, value, "ftShort") ? 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
        
    
    public boolean set_int(int col_num, Integer value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putInt(_validate_set(col_num, value, "ftInt") ? 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
     

    public boolean set_long(int col_num, Long value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putLong(_validate_set(col_num, value, "ftLong") ? (long) 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
     
    
    public boolean set_float(int col_num, Float value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putFloat(_validate_set(col_num, value, "ftFloat") ? (float)0.0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_double(int col_num, Double value) throws ConnException {  col_num--;
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putDouble(_validate_set(col_num, value, "ftDouble") ? 0.0 : value);
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_varchar(int col_num, String value) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        // Set actual value - padding with spaces to the left if needed
        string_bytes = _validate_set(col_num, value, "ftVarchar") ? "".getBytes(varchar_encoding) : value.getBytes(varchar_encoding);
        if (string_bytes.length > col_sizes[col_num]) 
            throw new ConnException("Trying to set string of size " + string_bytes.length + " on column of size " +  col_sizes[col_num] );
        // Generate missing spaces to fill up to size
        spaces = new byte[col_sizes[col_num] - string_bytes.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space
        
        // Set value and added spaces if needed
        data_columns[col_num].put(string_bytes);
        data_columns[col_num].put(spaces);
        // data_columns[col_num].put(String.format("%-" + col_sizes[col_num] + "s", value).getBytes());
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_nvarchar(int col_num, String value) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        // Convert string to bytes
        string_bytes = _validate_set(col_num, value, "ftBlob") ? "".getBytes(UTF8) : value.getBytes(UTF8);
                
        // Add string length to lengths column
        nvarc_len_columns[col_num].putInt(string_bytes.length);
        
        // Set actual value
        data_columns[col_num].put(string_bytes);
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_date(int col_num, Date date, ZoneId zone) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        
    	// Set actual value
        data_columns[col_num].putInt(_validate_set(col_num, date, "ftDate") ? 0 : dateToInt(date, zone));
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
        
    
    public boolean set_datetime(int col_num, Timestamp ts, ZoneId zone) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        
    	//ZonedDateTime dt = ts.toLocalDateTime().atZone(zone); 
    	// ZonedDateTime dt = ts.toInstant().atZone(zone); 

    	// Set actual value
        data_columns[col_num].putLong(_validate_set(col_num, ts, "ftDateTime") ? 0 : dtToLong(ts, zone));
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_date(int col_num, Date value) throws ConnException, UnsupportedEncodingException { 
        
        return set_date(col_num, value, system_tz); // system_tz, UTC
    }
        
    
    public boolean set_datetime(int col_num, Timestamp value) throws ConnException, UnsupportedEncodingException {  
    	
        return set_datetime(col_num, value, system_tz); // system_tz, UTC
}
    
    // Metadata
    // --------
    
    int _validate_col_num(int col_num) throws ConnException {
        
        if (col_num <1) 
            throw new ConnException ("Using a metadata function with a non positive column value");
        
        return --col_num;
    }

    @Override
    public int getStatementId() {
        return statementId;
    }

    @Override
    public String getQueryType() {
        
        return statement_type;
    }

    @Override
    public int getRowLength() {  // number of columns for this query
        
        return row_length;
    }

    @Override
    public String getColName(int col_num) throws ConnException {
    
        return col_names[_validate_col_num(col_num)];
    }

    @Override
    public String get_col_type(int col_num) throws ConnException {
        return col_types[_validate_col_num(col_num)];
    }

    @Override
    public String get_col_type(String col_name) throws ConnException {
    	Integer col_num = col_names_map.get(col_name);
    	if (col_num == null)
    		throw new ConnException("\nno column found for name: " + col_name + "\nExisting columns: \n" + col_names_map.keySet());
        return get_col_type(col_names_map.get(col_name));
    }

    @Override
    public int get_col_size(int col_num) throws ConnException {  
    
        return col_sizes[_validate_col_num(col_num)];
    }

    @Override
    public boolean is_col_nullable(int col_num) throws ConnException { 
        
        return col_nullable.get(_validate_col_num(col_num));
    }

    @Override
    public boolean isOpenStatement() {
        return openStatement;
    }

    @Override
    public boolean isOpen() {
        return s.isOpen();
    }

    @Override
    public AtomicBoolean checkCancelStatement() {
        return this.IsCancelStatement;
    }

    @Override
    public void setOpenStatement(boolean openStatement) {
        this.openStatement = openStatement;
    }

    @Override
    public boolean setFetchLimit(int _fetch_limit) throws ConnException{
    	
    	if (_fetch_limit < 0)
			throw new ConnException("Max rows to fetch should be nonnegative, got" + _fetch_limit);

    	fetch_limit = _fetch_limit;
    	
    	return true;
    }

    @Override
    public int getFetchLimit() {
    	return fetch_limit;
    }

}
