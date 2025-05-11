package org.apache.ignite.console.agent.db;


import java.sql.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.ignite.console.agent.utils.SqlStringUtils;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by author on 14-08-2015.
 *
 * @author Rajasekhar
 */
public class JdbcQueryExecutor implements Callable<JsonObject> {

    private final Statement statement;
    
    private final String sql;
    
    private final String schema;
    
    private final boolean isQuery;

    public JdbcQueryExecutor(Statement statement, String sql,String schema) {
        this.statement = statement;
        this.sql = sql;
        this.schema = schema;
        String nomalSQL = SqlStringUtils.removeSQLComments(sql).trim().toLowerCase();
        this.isQuery = nomalSQL.startsWith("select") || nomalSQL.startsWith("show") || nomalSQL.startsWith("describe");
        
    }
    
     

    @Override
    public JsonObject call() throws SQLException {
    	if(this.isQuery) {
    		return executeSqlVisor(0,"");
    	}
    	else {
    		return executeSql(0,"");
    	}
    }
    
    public JsonObject call(int queryId,String nodeId) throws SQLException {
    	if(this.isQuery) {
    		return executeSqlVisor(0,nodeId);
    	}
    	else {
    		return executeSql(queryId,nodeId);
    	}
    }
    
    public JsonObject executeSql(int queryId,String nodeId) throws SQLException {
    	ResultSet resultSet = null;
        long start = System.currentTimeMillis();        
        JsonObject queryResult = new JsonObject();
        JsonArray metaDataArray = new JsonArray();
        JsonArray dataArray = new JsonArray();
        
        String err = null;
        try {
        	queryResult.put("hasMore", false);
            boolean isResult = this.statement.execute(this.sql);
            if(isResult) {
	            resultSet = this.statement.getResultSet();
	            if(resultSet!=null) {
	            	ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		            int rowCount = 0; //To count the number of rows		
		            
		            int columnCount = resultSetMetaData.getColumnCount();
		
		            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was
		            // called late.
		            addFieldsMetadata(resultSetMetaData, metaDataArray, columnCount);
		
		            
		            while (resultSet.next()) {
		            	JsonArray row = new JsonArray();
		                ++rowCount;
		                for (int index = 1; index <= columnCount; index++) {
		                    //int columnType = resultSetMetaData.getColumnType(index);
		                    Object object = resultSet.getObject(index);
		                    row.add(object);
		                }
		                dataArray.add(row);
		            }
	            }
	            
	            boolean hasMore = this.statement.getMoreResults();
	            queryResult.put("hasMore", hasMore);
            }
            else {
            	int effectCount = this.statement.getUpdateCount();            	
            	JsonObject object = new JsonObject();
    			object.put("fieldName", "effectCount");
    			object.put("typeName", "");
    			object.put("schemaName", this.schema);	    			
    			object.put("fieldTypeName", "Integer");
    			metaDataArray.add(object);
    			
    			JsonArray row = new JsonArray();
    			row.add(effectCount);
    			dataArray.add(row);
            }
            
            
            queryResult.put("queryId", queryId);
            queryResult.put("responseNodeId", nodeId);
            
            queryResult.put("rows", dataArray); 
            queryResult.put("columns", metaDataArray);
            queryResult.put("protocolVersion", 1);
          
            long end = System.currentTimeMillis();
            queryResult.put("duration", end-start);
            
            return queryResult;
            
        } catch (SQLException ex) {
        	err = ex.getMessage();
        	queryResult.put("error",err);        
		} finally {
            if(null!=resultSet) resultSet.close();
        }        
        
        return queryResult;
    }
    
    /**
     * Ignite兼容模型
     * @param queryId
     * @param nodeId
     * @return
     * @throws SQLException
     */
    public JsonObject executeSqlVisor(int queryId,String nodeId) throws SQLException {
        ResultSet resultSet = null;
        long start = System.currentTimeMillis();        
        JsonObject queryResult = new JsonObject();
        String err = null;
        try {
            resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int rowCount = 0; //To count the number of rows
            
            queryResult.put("hasMore", false);
            queryResult.put("queryId", queryId);
            queryResult.put("responseNodeId", nodeId);
            

            JsonArray metaDataArray = new JsonArray();
            int columnCount = resultSetMetaData.getColumnCount();

            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was
            // called late.
            addFieldsMetadata(resultSetMetaData, metaDataArray, columnCount);

            JsonArray dataArray = new JsonArray();
            while (resultSet.next()) {
            	JsonArray row = new JsonArray();
                ++rowCount;
                for (int index = 1; index <= columnCount; index++) {
                    //int columnType = resultSetMetaData.getColumnType(index);
                    Object object = resultSet.getObject(index);
                    row.add(object);
                }
                dataArray.add(row);
            }
            queryResult.put("rows", dataArray); 
            queryResult.put("columns", metaDataArray);
            queryResult.put("protocolVersion", 1);
          
            long end = System.currentTimeMillis();
            queryResult.put("duration", end-start);
            
            return queryResult;
            
        } catch (SQLException ex) {
        	err = ex.getMessage();
        	queryResult.put("error",err);        
		} finally {
            if(null!=resultSet) resultSet.close();
        }        
        
        return queryResult;
    }

    
    public JsonObject executeSqlList() throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int rowCount = 0; //To count the number of rows

            JsonObject queryResult = new JsonObject();
            queryResult.put("last", false);
            queryResult.put("queryId", 0);

            JsonArray metaDataArray = new JsonArray();
            int columnCount = resultSetMetaData.getColumnCount();

            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was
            // called late.
            addFieldsMetadata(resultSetMetaData, metaDataArray, columnCount);

            JsonArray dataArray = new JsonArray();
            while (resultSet.next()) {
            	JsonArray row = new JsonArray();
                ++rowCount;
                for (int index = 1; index <= columnCount; index++) {
                    //int columnType = resultSetMetaData.getColumnType(index);
                    Object object = resultSet.getObject(index);
                    row.add(object);
                }
                dataArray.add(row);
            }
            queryResult.put("items", dataArray); 
            queryResult.put("fieldsMetadata", metaDataArray);
            return queryResult;
        } catch (SQLException ex) {
            throw new SQLException("Couldn't query the database", ex);        
		} finally {
            if(null!=resultSet) resultSet.close();
        }
    }

    public JsonObject executeSqlObject() throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int rowCount = 0; //To count the number of rows

            JsonObject queryResult = new JsonObject();

            JsonArray metaDataArray = new JsonArray();
            int columnCount = resultSetMetaData.getColumnCount();

            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was called late.
            addMetadata(resultSetMetaData, metaDataArray, columnCount);

            JsonArray dataArray = new JsonArray();
            while (resultSet.next()) {
                JsonObject row = new JsonObject();
                ++rowCount;
                addARow(resultSet, resultSetMetaData, columnCount, dataArray, row);
            }
            queryResult.put("data", dataArray);

            JsonObject rowsJson = new JsonObject();
            rowsJson.put("rows", rowCount);
            metaDataArray.add(rowsJson);

            queryResult.put("metadata", metaDataArray);
            return queryResult;
        } catch (SQLException ex) {
            throw new SQLException("Couldn't query the database", ex);        
		} finally {
            if(null!=resultSet) resultSet.close();
        }
    }
    
	private void addFieldsMetadata(ResultSetMetaData resultSetMetaData, JsonArray metaDataArray, int columnCount)
			throws SQLException {
		for (int counter = 1; counter <= columnCount; counter++) {
			JsonObject object = new JsonObject();
			object.put("fieldName", resultSetMetaData.getColumnLabel(counter));
			object.put("typeName", resultSetMetaData.getTableName(counter));
			object.put("schemaName", resultSetMetaData.getSchemaName(counter));			
			object.put("fieldTypeName", resultSetMetaData.getColumnTypeName(counter));
			metaDataArray.add(object);
		}		
	}

    private void addMetadata(ResultSetMetaData resultSetMetaData, JsonArray metaDataArray,
                             int columnCount) throws SQLException {
        
        JsonObject columnNameAndType = new JsonObject();

        for (int counter = 1; counter <= columnCount; counter++) {
            JsonObject object = new JsonObject();
            object.put("name", resultSetMetaData.getColumnLabel(counter));
            object.put("type", resultSetMetaData.getColumnTypeName(counter));

            //int columnType = resultSetMetaData.getColumnType(counter);
            //final String aClass = resultSetMetaData.getColumnTypeName(counter);
            //int pos = aClass.lastIndexOf(".");

            columnNameAndType.put(Integer.toString(counter), object);
        }
        metaDataArray.add(columnNameAndType);
    }

    private void addARow(ResultSet resultSet, ResultSetMetaData resultSetMetaData, int columnCount,
                         JsonArray dataArray, JsonObject row) throws SQLException {
        String nullValue = null;
        for (int index = 1; index <= columnCount; index++) {
            int columnType = resultSetMetaData.getColumnType(index);
            Object object = resultSet.getObject(index);
            String columnLabel = resultSetMetaData.getColumnLabel(index);
            if ((columnType == Types.DATE) || (columnType == Types.TIMESTAMP) || (columnType == Types.TIME)) {
                if (object == null) {
                    row.put(columnLabel, nullValue);
                } else {
                    row.put(columnLabel, object.toString());
                }
            } else {
                if (object instanceof Number) {
                    row.put(columnLabel, (Number) (object));
                } else if (object instanceof Character) {
                    row.put(columnLabel, (Character) (object));
                } else if (object instanceof Boolean) {
                    //UI Needs as string
                    row.put(columnLabel, "" + object);
                } else {
                    row.put(columnLabel, (object == null ? nullValue : object.toString()));
                }
            }
        }
        dataArray.add(row);
    }
}