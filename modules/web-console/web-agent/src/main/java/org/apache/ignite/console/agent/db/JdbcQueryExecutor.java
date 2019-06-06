package org.apache.ignite.console.agent.db;


import java.sql.*;
import java.util.Map;
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
    public JSONObject call() throws Exception {
        return executeSql();
    }

    public JSONObject executeSql() throws SQLException {
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
            if(null!=resultSet); resultSet.close();
        }
    }

    private void addMetadata(ResultSetMetaData resultSetMetaData, JSONArray metaDataArray,
                             int columnCount) throws SQLException, JSONException {
        
        JSONObject columnNameAndType = new JSONObject();

        for (int counter = 1; counter <= columnCount; counter++) {
            JSONObject object = new JSONObject();
            object.put("name", resultSetMetaData.getColumnLabel(counter));

            final int columnType = resultSetMetaData.getColumnType(counter);
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