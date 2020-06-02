package com.sqream.jdbc;

import java.sql.ParameterMetaData;
import java.sql.Types;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnException;

import static com.sqream.jdbc.connector.enums.StatementType.INSERT;


public class SQParameterMetaData implements ParameterMetaData{
	private static final Logger LOGGER = Logger.getLogger(SQParameterMetaData.class.getName());
	
	  Connector conn;
	  int param_count = 0;
	  
	  Map<String, Integer> sqream_to_sql = Stream.of(new Object[][] { 
		    { "ftBool", Types.BOOLEAN }, 
		    { "ftUByte", Types.TINYINT }, 
		    { "ftShort", Types.SMALLINT }, 
		    { "ftInt", Types.INTEGER }, 
		    { "ftLong", Types.BIGINT }, 
		    { "ftFloat", Types.REAL }, 
		    { "ftDouble", Types.DOUBLE }, 
		    { "ftDate", Types.DATE }, 
		    { "ftDateTime", Types.TIMESTAMP }, 
		    { "ftVarchar", Types.VARCHAR }, 
		    { "ftBlob", Types.NVARCHAR }, 
		}).collect(Collectors.toMap(data -> (String) data[0], data -> (Integer) data[1]));
	  
	  
	  
	  public SQParameterMetaData(Connector _conn) throws SQLException {
		  
	     if (_conn == null)
	    	 throw new SQLException("null connector object passed to SQParameterMetaData");
	     
	     conn = _conn;
	     // A query is marked "INSERT" in Connector.java when we get a non-empty queryTypeIn - network insert
    	 param_count = (conn.getQueryType().equals(INSERT.getValue())) ? conn.getRowLength() : 0;
    	 
	  }
	  
	  
	  boolean _validate_index (int param_index) throws SQLException {
		  
		  if (param_index > param_count || param_index < 1)
			  throw new SQLException ("parameter index " + param_index + " out of range. Number of parameters for this query is: " + param_count);
		  
		  return true;
	  }


	@Override
	public int getParameterCount() {
		LOGGER.log(Level.FINE, MessageFormat.format("getParameterCount() return [{0}]", param_count));
		return param_count;
	}

	  
	  @Override
	  public String getParameterClassName(int paramIndex) throws SQLException {
	    
		_validate_index(paramIndex);
	    try {
			String result = conn.getColName(paramIndex);
			LOGGER.log(Level.FINE, MessageFormat.format("getParameterClassName() return [{0}] for paramIndex", result, paramIndex));
			return result;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to get parameter name in SQParameterMetaData:\n" + e);
		}
	    
	  }
	  
	  
	  @Override
	  public int getParameterType(int paramIndex) throws SQLException {
	   
		_validate_index(paramIndex);

		try {
			int result = sqream_to_sql.get(conn.getColType(paramIndex));
			LOGGER.log(Level.FINE, MessageFormat.format("getParameterType() return [{0}] for paramIndex", result, paramIndex));
			return result;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to check parameter type in SQParameterMetaData:\n" + e);
		}
	    
	  }
	  
	  
	  // ParameterModes explained here - 
	  // https://docs.oracle.com/javase/tutorial/jdbc/basics/storedprocedures.html
	  // Looks like we're always IN
	  @Override
	  public int getParameterMode(int param_index) throws SQLException {
	    
		  _validate_index(param_index);
	    
		  return ParameterMetaData.parameterModeIn;
	  }
	  
	  
	  @Override
	  public int getPrecision(int paramIndex) throws SQLException {
	    
		  _validate_index(paramIndex);
		  
		  try {
			  int result = conn.getColSize(paramIndex);
			  LOGGER.log(Level.FINE, MessageFormat.format("getPrecision return [{0}] for paramIndex [{1}]", result, paramIndex));
			  return result;
			} catch (ConnException e) {
				e.printStackTrace();
				throw new SQLException ("Connector exception when trying to check parameter precision in SQParameterMetaData:\n" + e);
			}
	  }
	  
	  
	  @Override
	  public int getScale(int param_index) throws SQLException {
	    LOGGER.log(Level.FINE, "getScale()");
		_validate_index(param_index);
		
		// Not applicable for SQream types - we only support float and double, no such quality
	    // If adding types to sqream, this may need to be revised
	    int param_scale = 0; 
	    
	    
	    return param_scale;
	  }
	  
	  
	  @Override
	  public int isNullable(int param_index) throws SQLException {
	    
		  _validate_index(param_index);
	    
	    try {
	    	return conn.isColNullable(param_index) ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNullableUnknown;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to get parameter nullability in SQParameterMetaData:\n" + e);
		}
	    
	  }
	  
	  
	  @Override
	  public String getParameterTypeName(int paramIndex) throws SQLException {
		  
		  _validate_index(paramIndex);
		  
		  try {
			  String result = conn.getColType(paramIndex);
			  LOGGER.log(Level.FINE, MessageFormat.format("getParameterTypeName return [{0}] for paramIndex [{1}]", result, paramIndex));
			  return result;
			} catch (ConnException e) {
				e.printStackTrace();
				throw new SQLException ("Connector exception when trying to get parameter type name in SQParameterMetaData:\n" + e);
			}
		    
	  }
	  
	  
	  @Override
	  public boolean isSigned(int paramIndex) throws SQLException {
	    
		  _validate_index(paramIndex);
	    
        try {
			boolean result = Arrays.asList("ftShort", "ftInt", "ftLong", "ftFloat", "ftDouble").contains(conn.getColType(paramIndex));
			LOGGER.log(Level.FINE, MessageFormat.format("isSigned return [{0}] for paramIndex [{1}]", result, paramIndex));
			return result;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to check if parameter is signed in SQParameterMetaData:\n" + e);
		}
	  
	  }

	  
	  // Methods inherited from interface java.sql.Wrapper
	  @Override
	  public boolean isWrapperFor(Class<?> iface) throws SQLException {
	    
		  throw new SQLFeatureNotSupportedException("isWrapperFor in SQParameterMetaData");
		  //return iface.isAssignableFrom(getClass());
	  }
	  
	  
	  // Methods inherited from interface java.sql.Wrapper
	  @Override
	  public <T> T unwrap(Class<T> iface) throws SQLException {
		  
		  throw new SQLFeatureNotSupportedException("unwrap in SQParameterMetaData");
		  
		  /*
		  if (iface.isAssignableFrom(getClass())) {
	      return iface.cast(this);
	    }
	    throw new SQLException("Cannot unwrap to " + iface.getName());  
	    //*/
	  }
	  
}
