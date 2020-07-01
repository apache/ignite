package org.apache.ignite.console.agent.db;


import java.sql.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by author on 14-08-2015.
 *
 * @author Rajasekhar
 */
public class JdbcQueryExecutor implements Callable<JSONObject> {

    private final Statement statement;
    
    private final String sql;

    public JdbcQueryExecutor(Statement statement, String sql) {
        this.statement = statement;
        this.sql = sql;
        
    }

    @Override
    public JSONObject call() throws SQLException {
        return executeSqlList();
    }    
    

    public JSONObject executeSqlVisor(int queryId,String nodeId) throws SQLException {
        ResultSet resultSet = null;
        long start = System.currentTimeMillis();
        JSONObject res = new JSONObject();
        JSONObject result = new JSONObject();
        String err = null;
        try {
            resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int rowCount = 0; //To count the number of rows

            JSONObject queryResult = new JSONObject();
            queryResult.put("hasMore", false);
            queryResult.put("queryId", queryId);
            queryResult.put("responseNodeId", nodeId);
            

            JSONArray metaDataArray = new JSONArray();
            int columnCount = resultSetMetaData.getColumnCount();

            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was
            // called late.
            addFieldsMetadata(resultSetMetaData, metaDataArray, columnCount);

            JSONArray dataArray = new JSONArray();
            while (resultSet.next()) {
            	JSONArray row = new JSONArray();
                ++rowCount;
                for (int index = 1; index <= columnCount; index++) {
                    //int columnType = resultSetMetaData.getColumnType(index);
                    Object object = resultSet.getObject(index);
                    row.put(object);
                }
                dataArray.put(row);
            }
            queryResult.put("rows", dataArray); 
            queryResult.put("columns", metaDataArray);
            queryResult.put("protocolVersion", 1);
          
            long end = System.currentTimeMillis();
            queryResult.put("duration", end-start);
            
            
            result.put("result", queryResult);
            result.put("protocolVersion", 1);
            res.put("result", result);
            
        } catch (SQLException ex) {
        	err = ex.getMessage();            
        } catch (JSONException ex) {			
        	throw new SQLException("Couldn't build the database json", ex);
		} finally {
            if(null!=resultSet) resultSet.close();
        }
        
        try {
			result.put("error",err);
			res.put("error",err);
			res.put("finished",true);
			res.put("id","~"+nodeId);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return res;
    }

    
    public JSONObject executeSqlList() throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int rowCount = 0; //To count the number of rows

            JSONObject queryResult = new JSONObject();
            queryResult.put("last", false);
            queryResult.put("queryId", 0);

            JSONArray metaDataArray = new JSONArray();
            int columnCount = resultSetMetaData.getColumnCount();

            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was
            // called late.
            addFieldsMetadata(resultSetMetaData, metaDataArray, columnCount);

            JSONArray dataArray = new JSONArray();
            while (resultSet.next()) {
            	JSONArray row = new JSONArray();
                ++rowCount;
                for (int index = 1; index <= columnCount; index++) {
                    //int columnType = resultSetMetaData.getColumnType(index);
                    Object object = resultSet.getObject(index);
                    row.put(object);
                }
                dataArray.put(row);
            }
            queryResult.put("items", dataArray); 
            queryResult.put("fieldsMetadata", metaDataArray);
            return queryResult;
        } catch (SQLException ex) {
            throw new SQLException("Couldn't query the database", ex);
        } catch (JSONException ex) {			
        	throw new SQLException("Couldn't build the database json", ex);
		} finally {
            if(null!=resultSet) resultSet.close();
        }
    }

    public JSONObject executeSqlObject() throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int rowCount = 0; //To count the number of rows

            JSONObject queryResult = new JSONObject();

            JSONArray metaDataArray = new JSONArray();
            int columnCount = resultSetMetaData.getColumnCount();

            //Adding metadata of the result. This is a fix for SQLite. Earlier the method was
            // called late.
            addMetadata(resultSetMetaData, metaDataArray, columnCount);

            JSONArray dataArray = new JSONArray();
            while (resultSet.next()) {
                JSONObject row = new JSONObject();
                ++rowCount;
                addARow(resultSet, resultSetMetaData, columnCount, dataArray, row);
            }
            queryResult.put("data", dataArray);

            JSONObject rowsJson = new JSONObject();
            rowsJson.put("rows", rowCount);
            metaDataArray.put(rowsJson);

            queryResult.put("metadata", metaDataArray);
            return queryResult;
        } catch (SQLException ex) {
            throw new SQLException("Couldn't query the database", ex);
        } catch (JSONException ex) {			
        	throw new SQLException("Couldn't build the database json", ex);
		} finally {
            if(null!=resultSet) resultSet.close();
        }
    }
    
	private void addFieldsMetadata(ResultSetMetaData resultSetMetaData, JSONArray metaDataArray, int columnCount)
			throws SQLException, JSONException {
		for (int counter = 1; counter <= columnCount; counter++) {
			JSONObject object = new JSONObject();
			object.put("fieldName", resultSetMetaData.getColumnLabel(counter));
			object.put("typeName", resultSetMetaData.getTableName(counter));
			object.put("schemaName", resultSetMetaData.getSchemaName(counter));	
			final String aClass = resultSetMetaData.getColumnClassName(counter);			
			object.put("fieldTypeName", aClass);
			metaDataArray.put(object);
		}		
	}

    private void addMetadata(ResultSetMetaData resultSetMetaData, JSONArray metaDataArray,
                             int columnCount) throws SQLException, JSONException {
        
        JSONObject columnNameAndType = new JSONObject();

        for (int counter = 1; counter <= columnCount; counter++) {
            JSONObject object = new JSONObject();
            object.put("name", resultSetMetaData.getColumnLabel(counter));

            int columnType = resultSetMetaData.getColumnType(counter);
            final String aClass = resultSetMetaData.getColumnClassName(counter);
            int pos = aClass.lastIndexOf(".");
            object.put("type", aClass.substring(pos+1));

            columnNameAndType.put(Integer.toString(counter), object);
        }
        metaDataArray.put(columnNameAndType);
    }

    private void addARow(ResultSet resultSet, ResultSetMetaData resultSetMetaData, int columnCount,
                         JSONArray dataArray, JSONObject row) throws SQLException, JSONException {
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
        dataArray.put(row);
    }
}