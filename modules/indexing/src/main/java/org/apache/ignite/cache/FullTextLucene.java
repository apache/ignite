package org.apache.ignite.cache;
/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2TableEngine;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneDirectory;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.h2.api.Trigger;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

import java.io.File;
import java.nio.file.Paths;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.IndexWriter;


/**
 * This class implements the full text search based on Apache Lucene.
 * Most methods can be called using SQL statements as well.
 */
public class FullTextLucene {
	
    public static class FullTextIndexKey implements java.io.Serializable{
    	public String schema;
    	public String table;    
    };
    
    
    public static class FullTextIndex implements java.io.Serializable{
    	public String schema;
    	public String table;
    	public String columns;
    };

	 /**
     * A column name of the result set returned by the searchData method.
     */
	public static final String FIELD_SCHEMA = "_SCHEMA";

    /**
     * A column name of the result set returned by the searchData method.
     */
    public static final String FIELD_TABLE = "_TABLE";

    /**
     * A column name of the result set returned by the searchData method.
     */
    public static final String FIELD_COLUMNS = "_COLUMNS";

    /**
     * A column name of the result set returned by the searchData method.
     */
    public static final String FIELD_KEY = "_KEY";

    /**
     * The hit score.
     */
    public static final String FIELD_SCORE = "_SCORE";
   

    private static final HashMap<String, LuceneIndexAccess> INDEX_ACCESS = new HashMap<>();
    private static final String TRIGGER_PREFIX = "FTL_";
    private static final String SCHEMA = "\"FTL\"";
   
    public static final String LUCENE_FIELD_MODIFIED = "_modified";
    
    /** Field name for value expiration time. */
    public static final String EXPIRATION_TIME_FIELD_NAME = "_expires";
   
    /**
     * The prefix for a in-memory path. This prefix is only used internally
     * within this class and not related to the database URL.
     */
    public static final String IN_MEMORY_PREFIX = "mem:";
    
   
    public static GridKernalContext ctx = null;    
   
    
    private static String cacheName(String schema,String table){
    	if(schema==null || schema.isEmpty()){
    		return table;
    	}
    	//if(ctx.cache().cache(schema)!=null){
    	//	return schema;
    	//};
    	return "SQL_"+schema+"_"+table.toUpperCase();
    }   
  

    /**
     * @param bytes Bytes.
     * @param ldr Class loader.
     * @return Object.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private static <Z> Z unmarshall(byte[] bytes, ClassLoader ldr,CacheObjectContext coctx) throws IgniteCheckedException {
        if (coctx == null) // For tests.
            return (Z)JdbcUtils.deserialize(bytes, null);

        return (Z)ctx.cacheObjects().unmarshal(coctx, bytes, ldr);
    }
    
    
    

    
    /**
     * Initializes full text search functionality for this database. This adds
     * the following Java functions to the database:
     * <ul>
     * <li>FTL_CREATE_INDEX(schemaNameString, tableNameString,
     * columnListString)</li>
     * <li>FTL_SEARCH(queryString, limitInt, offsetInt): result set</li>
     * <li>FTL_REINDEX()</li>
     * <li>FTL_DROP_ALL()</li>
     * </ul>
     * It also adds a schema FTL to the database where bookkeeping information
     * is stored. This function may be called from a Java application, or by
     * using the SQL statements:
     *
     * <pre>
     * CREATE ALIAS IF NOT EXISTS FTL_INIT FOR
     *      &quot;org.h2.fulltext.FullTextLucene.init&quot;;
     * CALL FTL_INIT();
     * </pre>
     *
     * @param conn the connection
     */
    static boolean inited = false;
    
    @QuerySqlFunction(alias="ftl_init")
    public static void init(Connection conn) throws SQLException {    	
    	if(inited) return;
    	
    	if (ctx == null){ //not init ctx.
    		execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            
            String sql = "CREATE TABLE IF NOT EXISTS " + SCHEMA +
                    ".INDEXES(SCHEMA VARCHAR, TABLE VARCHAR,COLUMNS VARCHAR," +
                    "PRIMARY KEY(SCHEMA, TABLE))"; //+ " engine \"" + H2TableEngine.class.getName() + "\"";        
                   	
            execute(sql);
            
    		throw throwException("GridKernalContext is null. ignite index maybe not start. ");
    	}
        
        // table create by config.
        
        IgniteH2Indexing idxing = (IgniteH2Indexing)ctx.query().getIndexing();
        //idxing.registerType(cctx, type)
        	
        execute("CREATE ALIAS IF NOT EXISTS FTL_CREATE_INDEX FOR \"" +
                FullTextLucene.class.getName() + ".createIndex\"");
        execute("CREATE ALIAS IF NOT EXISTS FTL_DROP_INDEX FOR \"" +
                FullTextLucene.class.getName() + ".dropIndex\"");
        execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH FOR \"" +
                FullTextLucene.class.getName() + ".search\"");
        execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH_DATA FOR \"" +
                FullTextLucene.class.getName() + ".searchData\"");
        execute("CREATE ALIAS IF NOT EXISTS FTL_REINDEX FOR \"" +
                FullTextLucene.class.getName() + ".reindex\"");
        execute("CREATE ALIAS IF NOT EXISTS FTL_DROP_ALL FOR \"" +
                FullTextLucene.class.getName() + ".dropAll\"");  
        
        
        inited = true;
        
    }

    
    /**
     * Create cache type metadata for {@link index}.
     *
     * @return Cache type metadata.
     */
    private static QueryEntity createIndexQueryEntity() {
        QueryEntity indexEntity = new QueryEntity();

        indexEntity.setValueType(FullTextIndex.class.getName());
        indexEntity.setKeyType(FullTextIndexKey.class.getName());
        indexEntity.setKeyFields(new HashSet<>());
        indexEntity.getKeyFields().add("schema");
        indexEntity.getKeyFields().add("table");
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        
        fields.put("schema", String.class.getName());       
        fields.put("table", String.class.getName());
        fields.put("columns", String.class.getName());
        indexEntity.setFields(fields);
        return indexEntity;
    }

    public static void execute(String sql) throws SQLException{
    	
    	 try {
    		IgniteH2Indexing idxing = (IgniteH2Indexing)ctx.query().getIndexing();
			idxing.executeStatement("PUBLIC", sql);			
			
		} catch (IgniteCheckedException e) {
			// TODO Auto-generated catch block
			throwException(e.getMessage());
		}   
    }
    
    public static List<List<?>> querySql(String sql) throws SQLException{
    	
   	 try {
   		 	SqlFieldsQuery qry = new SqlFieldsQuery(sql);			
			
			return ctx.query().querySqlFields(qry, true).getAll();
			
		} catch (IgniteException e) {
			// TODO Auto-generated catch block
			throwException(e.getMessage());
		}  
   	 	return null;
   }
    
    /**
     * Create a new full text index for a table and column list. Each table may
     * only have one index at any time.
     *
     * @param conn the connection
     * @param schema the schema name of the table (case sensitive)
     * @param table the table name (case sensitive)
     * @param columnList the column list (null for all columns)
     * 
     * @return index_name also equals cachename
     */
    @QuerySqlFunction(alias="FTL_CREATE_INDEX")
    public static String createIndex(Connection conn, String schema,
            String table, String columnList) throws SQLException {
    	init(conn);
    	//TODO@byron use ignite sql api
        String prep = String.format("INSERT INTO " + SCHEMA
                + ".INDEXES(SCHEMA, TABLE, COLUMNS) VALUES('%s', '%s', '%s')",schema,table,columnList);
       
        querySql(prep);
        
        createTrigger(conn, schema, table);
        indexExistingRows(conn, schema, table);
        
        return cacheName(schema,table);
    }

    
    /**
     * Drop an existing full text index for a table. This method returns
     * silently if no index for this table exists.
     *
     * @param conn the connection
     * @param schema the schema name of the table (case sensitive)
     * @param table the table name (case sensitive)
     * 
     * @return index_name also equals cachename
     */
    @QuerySqlFunction(alias="FTL_DROP_INDEX")
    public static void dropIndex(Connection conn, String schema, String table)
            throws SQLException {       

        //PreparedStatement prep = conn.prepareStatement("DELETE FROM " + SCHEMA
        //        + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
        //prep.setString(1, schema);
        //prep.setString(2, table);
        
        String prep = String.format("DELETE FROM " + SCHEMA
                + ".INDEXES WHERE SCHEMA='%s' AND TABLE='%s'",schema,table);
       
        
        
        int rowCount = querySql(prep).size();
        if (rowCount == 0) {
            return;
        }

        reindex(conn,schema, table);
    }
    
    /**
     * Re-creates the full text index for this database. Calling this method is
     * usually not needed, as the index is kept up-to-date automatically.
     *
     * @param conn the connection
     */
    @QuerySqlFunction(alias="FTL_REINDEX")
    public static void reindex(Connection conn,String forschema,String table) throws SQLException {
    	init(conn);
        removeAllTriggers(conn, TRIGGER_PREFIX,forschema,table);
        removeIndexFiles(conn,forschema,table);
        createTrigger(conn, forschema, table);
        indexExistingRows(conn, forschema, table);
    }

    /**
     * Re-creates the full text index for this database. Calling this method is
     * usually not needed, as the index is kept up-to-date automatically.
     *
     * @param conn the connection
     */
    @QuerySqlFunction(alias="FTL_REINDEX_ALL")
    public static void reindex(Connection conn,String forschema) throws SQLException {
    	init(conn);
        removeAllTriggers(conn, TRIGGER_PREFIX,forschema,null);
        
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " + SCHEMA + ".INDEXES");
        while (rs.next()) {
            String schema = rs.getString("SCHEMA");
            String table = rs.getString("TABLE");
            
            removeIndexFiles(conn,forschema,table);
            createTrigger(conn, schema, table);
            indexExistingRows(conn, schema, table);
        }
    }

    /**
     * Drops all full text indexes from the database.
     *
     * @param conn the connection
     */
    @QuerySqlFunction(alias="FTL_DROP_ALL")
    public static void dropAll(Connection conn,String forschema) throws SQLException {
    	init(conn);
    	PreparedStatement prep = conn.prepareStatement("DELETE FROM " + SCHEMA + ".INDEXES WHERE SCHEMA=?");
        prep.setString(1, forschema);
      
        int rowCount = prep.executeUpdate();
        if (rowCount == 0) {
            return;
        }
        //Statement stat = conn.createStatement();
        //stat.execute("DROP SCHEMA IF EXISTS " + SCHEMA);
        removeAllTriggers(conn, TRIGGER_PREFIX, forschema,null);
        
        //-removeIndexFiles(conn,forschema);
    }

    /**
     * Remove all triggers that start with the given prefix.
     *
     * @param conn the database connection
     * @param prefix the prefix
     */
    protected static void removeAllTriggers(Connection conn, String prefix,String forschema,String table)
            throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TRIGGERS");
        Statement stat2 = conn.createStatement();
        while (rs.next()) {
            String schema = rs.getString("TRIGGER_SCHEMA");
            String name = rs.getString("TRIGGER_NAME");
            if(forschema!=null){
            	if(!forschema.equalsIgnoreCase(schema)){
            		continue;
            	}
            }
            if(table!=null){
            	String triggerTab =  StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
            	if(!name.startsWith(triggerTab)){
            		continue;
            	}
            }
            if (name.startsWith(prefix)) {
                name = StringUtils.quoteIdentifier(schema) + "." +
                        StringUtils.quoteIdentifier(name);
                stat2.execute("DROP TRIGGER " + name);
            }
        }
    }
    
    /**
     * Searches from the full text index for this database.
     * The returned result set has the following column:
     * <ul><li>QUERY (varchar): the query to use to get the data.
     * The query does not include 'SELECT * FROM '. Example:
     * PUBLIC.TEST WHERE ID = 1
     * </li><li>SCORE (float) the relevance score as returned by Lucene.
     * </li></ul>
     *
     * @param conn the connection
     * @param text the search query
     * @param limit the maximum number of rows or 0 for no limit
     * @param offset the offset or 0 for no offset
     * @return the result set
     */
    @QuerySqlFunction
    public static ResultSet search(Connection conn, String forschema, String table,String text, int limit,
            int offset) throws SQLException {
        return search(conn,forschema,table, text, limit, offset, false);
    }

    /**
     * Searches from the full text index for this database. The result contains
     * the primary key data as an array. The returned result set has the
     * following columns:
     * <ul>
     * <li>SCHEMA (varchar): the schema name. Example: PUBLIC</li>
     * <li>TABLE (varchar): the table name. Example: TEST</li>
     * <li>COLUMNS (array of varchar): comma separated list of quoted column
     * names. The column names are quoted if necessary. Example: (ID)</li>
     * <li>KEYS (array of values): comma separated list of values.
     * Example: (1)</li>
     * <li>SCORE (float) the relevance score as returned by Lucene.</li>
     * </ul>
     *
     * @param conn the connection
     * @param text the search query
     * @param limit the maximum number of rows or 0 for no limit
     * @param offset the offset or 0 for no offset
     * @return the result set
     */
    @QuerySqlFunction
    public static ResultSet searchData(Connection conn, String forschema,String table, String text, int limit,
            int offset) throws SQLException {
        return search(conn, forschema, table, text, limit, offset, true);
    }

    /**
     * Convert an exception to a fulltext exception.
     *
     * @param e the original exception
     * @return the converted SQL exception
     */
    protected static SQLException convertException(Exception e) {
        SQLException e2 = new SQLException(
                "Error while indexing document", "FULLTEXT");
        e2.initCause(e);
        return e2;
    }

    /**
     * Create the trigger.
     *
     * @param conn the database connection
     * @param schema the schema name
     * @param table the table name
     */
    protected static void createTrigger(Connection conn, String schema,
            String table) throws SQLException {
    	if(ctx==null){ //only run not in ignite.
    		createOrDropTrigger(conn, schema, table, true);
    	}
    }

    private static void createOrDropTrigger(Connection conn,
            String schema, String table, boolean create) throws SQLException {
        Statement stat = conn.createStatement();
        String trigger = StringUtils.quoteIdentifier(schema) + "." +
                StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
        stat.execute("DROP TRIGGER IF EXISTS " + trigger);
        if (create) {
            StringBuilder buff = new StringBuilder(
                    "CREATE TRIGGER IF NOT EXISTS ");
            // the trigger is also called on rollback because transaction
            // rollback will not undo the changes in the Lucene index
            buff.append(trigger).
                append(" AFTER INSERT, UPDATE, DELETE, ROLLBACK ON ").
                append(StringUtils.quoteIdentifier(schema)).
                append('.').
                append(StringUtils.quoteIdentifier(table)).
                append(" FOR EACH ROW CALL \"").
                append(FullTextLucene.FullTextTrigger.class.getName()).
                append('\"');
            stat.execute(buff.toString());
        }
    }

    /**
     * Set the column indices of a set of keys.
     *
     * @param index the column indices (will be modified)
     * @param keys the key list
     * @param columns the column list
     */
    protected static void setColumns(int[] index, ArrayList<String> keys,
            ArrayList<String> columns) throws SQLException {
        for (int i = 0, keySize = keys.size(); i < keySize; i++) {
            String key = keys.get(i);
            int found = -1;
            int columnsSize = columns.size();
            for (int j = 0; found == -1 && j < columnsSize; j++) {
                String column = columns.get(j);
                if (column.equals(key)) {
                    found = j;
                }
            }
            if (found < 0) {
                throw throwException("Column not found: " + key);
            }
            index[i] = found;
        }
    }

    /**
     * Get the index writer/searcher wrapper for the given connection.
     *
     * @param conn the connection
     * @return the index access wrapper
     */
    public static LuceneIndexAccess getIndexAccess(Connection conn,String schema,String table)
            throws SQLException {
        String path = getIndexPath(conn,schema,table);
       
        LuceneConfiguration idxConfig = LuceneConfiguration.getConfiguration(ctx,schema,table); 
        
        synchronized (INDEX_ACCESS) {
            LuceneIndexAccess access = INDEX_ACCESS.get(path);
           
            if (access == null) {
                try {
                	access = new LuceneIndexAccess(idxConfig,path);                 
                    
                } catch (IOException e) {
                    throw convertException(e);
                }
                INDEX_ACCESS.put(path, access);
            }
            
            if (!access.writer.isOpen()) {
                try {
                	access.open(idxConfig,path);                 
                    
                } catch (IOException e) {
                    throw convertException(e);
                }               
            }
            
            //fill indexed fields
            if(access.fields.isEmpty()){
            	access.config = idxConfig;      
            	if(idxConfig.type()!=null && idxConfig.type().textIndex()!=null){
            		access.fields.addAll(idxConfig.type().textIndex().fields());
                }
                // read index desc form FTL.INDEXES
                if(conn!=null){
                	ArrayList<String> indexList = New.arrayList();
	                PreparedStatement prep = conn.prepareStatement(
	                        "SELECT COLUMNS FROM " + SCHEMA+ ".INDEXES WHERE SCHEMA=? AND TABLE=?");
	                prep.setString(1, schema);
	                prep.setString(2, table);
	                
	                ResultSet rs = prep.executeQuery();
	                if (rs.next()) {
	                    String cols = rs.getString(1);
	                    if (cols != null) {
	                        for (String s : StringUtils.arraySplit(cols, ',', true)) {
	                            indexList.add(s);
	                        }
	                        access.fields.addAll(indexList);
	                    }
	                    else if(access.type()!=null){ //add all field to fulltext indexs
	                    	for(String f: access.type().fields().keySet()){
	                    		if(!f.isEmpty() && f.charAt(0)!='_'){
	                    			access.fields.add(f);
	                    		}
	                    	}                    	
	                    }	                    
	                }  
                }
            }
            return access;
        }
    }

    /**
     * Get the path of the Lucene index for this database.
     *
     * @param conn the database connection
     * @return the path
     */
    protected static String getIndexPath(Connection conn,String schema,String table) throws SQLException {
    	String cacheName = cacheName(schema,table);
    	if(ctx!=null){ 
    		try {
				String subFolder = ctx.pdsFolderResolver().resolveFolders().folderName();
				String path = ctx.config().getWorkDirectory()+File.separator+"lucene"+File.separator+subFolder+File.separator+"cache-"+cacheName;
	        	return path;
	        	
			} catch (IgniteCheckedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw convertException(e);
			}    		
    		
    	}
    	return IN_MEMORY_PREFIX + conn.getCatalog()+'/'+cacheName;
    }

    /**
     * Add the existing data to the index.
     *
     * @param conn the database connection
     * @param schema the schema name
     * @param table the table name
     */
    protected static void indexExistingRows(Connection conn, String schema,
            String table) throws SQLException {
        FullTextLucene.FullTextTrigger existing = new FullTextLucene.FullTextTrigger();
        existing.init(conn, schema, null, table, false, Trigger.INSERT);
        String sql = "SELECT _key,_val,_ver,* FROM " + StringUtils.quoteIdentifier(schema) +
                "." + StringUtils.quoteIdentifier(table);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            existing.insert(row, false);
        }
        
        String path = getIndexPath(conn,schema,table);
        LuceneIndexAccess access = INDEX_ACCESS.get(path);
        try{
        	access.commitIndex();
        }
        catch(IOException e){
        	 throw convertException(e);
        }
    }

    private static void removeIndexFiles(Connection conn,String schema,String table) throws SQLException {
        String path = getIndexPath(conn,schema,table);
        LuceneIndexAccess access = INDEX_ACCESS.get(path);
        if (access != null) {
            removeIndexAccess(access, path);
        }
        if (!path.startsWith(IN_MEMORY_PREFIX)) {
            FileUtils.deleteRecursive(path, false);
        }
    }

    /**
     * Close the index writer and searcher and remove them from the index access
     * set.
     *
     * @param access the index writer/searcher wrapper
     * @param indexPath the index path
     */
    protected static void removeIndexAccess(LuceneIndexAccess access, String indexPath)
            throws SQLException {
        synchronized (INDEX_ACCESS) {
            try {
                INDEX_ACCESS.remove(indexPath);
            
                access.reader.close();
                access.writer.close();
            } catch (Exception e) {
                throw convertException(e);
            }
        }
    }


    /**
     * Create an empty search result and initialize the columns.
     *
     * @param data true if the result set should contain the primary key data as
     *            an array.
     * @return the empty result set
     */
    protected static SimpleResultSet createResultSet(boolean data,Map<String, Class<?>> fields) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn(FIELD_KEY, Types.OTHER, 0, 0);
        result.addColumn(QueryUtils.VER_FIELD_NAME, Types.VARCHAR, 0, 0);
        result.addColumn(QueryUtils.VAL_FIELD_NAME, Types.OTHER, 0, 0);        
        result.addColumn(FIELD_TABLE, Types.VARCHAR, 0, 0);
        result.addColumn(FIELD_SCORE, Types.FLOAT, 0, 0);
        if (data) {
            result.addColumn(FIELD_COLUMNS, Types.ARRAY, 0, 0);
            
        }         
        return result;
    }
    

    /**
     * INTERNAL.
     * Convert the object to a string.
     *
     * @param data the object
     * @param type the SQL type
     * @return the string
     */
    protected static String asString(Object data, int type) throws SQLException {
        if (data == null) {
            return "NULL";
        }
        switch (type) {
        case Types.BIT:
        case Types.BOOLEAN:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
        case Types.REAL:
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.LONGVARCHAR:
        case Types.CHAR:
        case Types.VARCHAR:
            return data.toString();
        case Types.CLOB:
            try {
                if (data instanceof Clob) {
                    data = ((Clob) data).getCharacterStream();
                }
                return IOUtils.readStringAndClose((Reader) data, -1);
            } catch (IOException e) {
                throw DbException.toSQLException(e);
            }
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BINARY:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        case Types.BLOB:
        case Types.STRUCT:
        case Types.REF:
        case Types.NULL:
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
            throw throwException("Unsupported column data type: " + type);
        default:
            return "";
        }
    }

    
    /**
     * Do the search.
     *
     * @param conn the database connection
     * @param text the query
     * @param limit the limit
     * @param offset the offset
     * @param data whether the raw data should be returned
     * @return the result set
     */
    protected static ResultSet search(Connection conn, String forschema, String table, String text,
            int limit, int offset, boolean data) throws SQLException {   
    	
    	LuceneIndexAccess access = getIndexAccess(conn,forschema,table);
    	
        SimpleResultSet result = createResultSet(data,access.type()==null? null: access.type().fields());
        
        if (conn.getMetaData().getURL().startsWith("jdbc:columnlist:")) {
            // this is just to query the result set columns
            return result;
        }
        if (text == null || text.trim().length() == 0) {
            return result;
        }
        try {
        	String cacheName = access.cacheName();
        	ClassLoader ldr = null;
            
            GridCacheAdapter cache = null;
            if (ctx != null){
            	cache = ctx.cache().internalCache(cacheName);            	
            }
            if (cache != null && ctx.deploy().enabled())
                ldr = cache.context().deploy().globalLoader();
            
            access.flush();
            
            // take a reference as the searcher may change
            IndexSearcher searcher = access.searcher;
            // reuse the same analyzer; it's thread-safe;
            // also allows subclasses to control the analyzer used.
            Analyzer analyzer = access.writer.getAnalyzer();           
            
            MultiFieldQueryParser parser = new MultiFieldQueryParser(access.fields.toArray(new String[access.fields.size()]), analyzer);
                      
            
            // Filter expired items.
            Query filter = LongPoint.newRangeQuery(EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(),Long.MAX_VALUE);

            BooleanQuery.Builder query = new BooleanQuery.Builder()
                .add(parser.parse(text), BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER);
            
            
            // Lucene 3 insists on a hard limit and will not provide
            // a total hits value. Take at least 100 which is
            // an optimal limit for Lucene as any more
            // will trigger writing results to disk.
            int maxResults = (limit == 0 ? 100 : limit) + offset;
            TopDocs docs = searcher.search(query.build(), maxResults);
            if (limit == 0) {
                limit = (int)docs.totalHits;
            }
            for (int i = 0, len = docs.scoreDocs.length;
                    i < limit && i + offset < docs.totalHits
                    && i + offset < len; i++) {
                ScoreDoc sd = docs.scoreDocs[i + offset];
                Document doc = searcher.doc(sd.doc);
                float score = sd.score;
                String tableName = doc.get(FIELD_TABLE);
                
                Object k = unmarshall(doc.getBinaryValue(FIELD_KEY).bytes, ldr,cache.context().cacheObjectContext());
                Object ver = doc.get(QueryUtils.VER_FIELD_NAME);
                
                if (data && cache!=null) {
                	
                	Object v = cache.get(k,false,false);
                	
                    result.addRow(
                            k,ver,v,tableName,score,
                            access.fields.toArray()
                            );
                } else {
                    result.addRow(k,ver,null,tableName,score);
                }
            }
        } catch (Exception e) {
            throw convertException(e);
        }
        return result;
    }
    

    /**
     * Check if a the indexed columns of a row probably have changed. It may
     * return true even if the change was minimal (for example from 0.0 to
     * 0.00).
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @param indexColumns the indexed columns
     * @return true if the indexed columns don't match
     */
    protected static boolean hasChanged(Object[] oldRow, Object[] newRow,
            int[] indexColumns) {
        for (int c : indexColumns) {
            Object o = oldRow[c], n = newRow[c];
            if (o == null) {
                if (n != null) {
                    return true;
                }
            } else if (!o.equals(n)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Trigger updates the index when a inserting, updating, or deleting a row.
     */
    public static class FullTextTrigger implements Trigger {

        protected String schema;
        protected String table;
        protected int[] keys;
        protected int[] indexColumns;
        protected String[] columns;
        protected int[] columnTypes;
        protected String indexPath;
        protected LuceneIndexAccess indexAccess;

        /**
         * INTERNAL
         */
        @Override
        public void init(Connection conn, String schemaName, String triggerName,
                String tableName, boolean before, int type) throws SQLException {
            this.schema = schemaName;
            this.table = tableName;
            this.indexPath = getIndexPath(conn,schemaName,table);
            this.indexAccess = getIndexAccess(conn,schemaName,table);
            ArrayList<String> keyList = New.arrayList();
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getColumns(null,
                    StringUtils.escapeMetaDataPattern(schemaName),
                    StringUtils.escapeMetaDataPattern(tableName),
                    null);
            ArrayList<String> columnList = New.arrayList();
            while (rs.next()) {
                columnList.add(rs.getString("COLUMN_NAME"));
            }
            columnTypes = new int[columnList.size()];
            columns = new String[columnList.size()];
            columnList.toArray(columns);
            rs = meta.getColumns(null,
                    StringUtils.escapeMetaDataPattern(schemaName),
                    StringUtils.escapeMetaDataPattern(tableName),
                    null);
            for (int i = 0; rs.next(); i++) {
                columnTypes[i] = rs.getInt("DATA_TYPE");
            }
            if (keyList.size() == 0) {
                rs = meta.getPrimaryKeys(null,
                        StringUtils.escapeMetaDataPattern(schemaName),
                        tableName);
                while (rs.next()) {
                	String keyCol = rs.getString("COLUMN_NAME");
                	if(!keyList.contains(keyCol))
                		keyList.add(keyCol);
                }
            }
            if (keyList.size() == 0) {
                throw throwException("No primary key for table " + tableName);
            }
            ArrayList<String> indexList = New.arrayList();
            PreparedStatement prep = conn.prepareStatement(
                    "SELECT COLUMNS FROM " + SCHEMA
                    + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
            prep.setString(1, schemaName);
            prep.setString(2, tableName);
            rs = prep.executeQuery();
            if (rs.next()) {
                String cols = rs.getString(1);
                if (cols != null) {
                    for (String s : StringUtils.arraySplit(cols, ',', true)) {
                        indexList.add(s);
                    }
                }
                
            }
            
            if (indexList.size() == 0) {
            	for(String f: columnList){
            		if(!f.isEmpty() && f.charAt(0)!='_'){
            			indexList.add(f);
            		}
            	}              
            }           
            this.indexAccess.fields.addAll(indexList);
            
            keys = new int[keyList.size()];
            setColumns(keys, keyList, columnList);
            indexColumns = new int[indexList.size()];
            setColumns(indexColumns, indexList, columnList);
        }

        /**
         * INTERNAL
         */
        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            if (oldRow != null) {
                if (newRow != null) {
                    // update
                    if (hasChanged(oldRow, newRow, indexColumns)) {
                        delete(oldRow, false);
                        insert(newRow, true);
                    }
                } else {
                    // delete
                    delete(oldRow, true);
                }
            } else if (newRow != null) {
                // insert
                insert(newRow, true);
            }
        }

        /**
         * INTERNAL
         */
        @Override
        public void close() throws SQLException {
            if (indexAccess != null) {
                removeIndexAccess(indexAccess, indexPath);
                indexAccess = null;
            }
        }

        /**
         * INTERNAL
         */
        @Override
        public void remove() {
            // ignore
        }

       

        /**
         * Add a row to the index.
         *
         * @param row the row
         * @param commitIndex whether to commit the changes to the Lucene index
         */
        protected void insert(Object[] row, boolean commitIndex) throws SQLException {
        	
        	
            Field.Store storeText = indexAccess.config.isStoreTextFieldValue() ?  Field.Store.YES : Field.Store.NO;
            
            Document doc = new Document();   
            
            BytesRef _key = getBytes(row,0);
            doc.add(new StringField(FIELD_KEY, _key, Field.Store.YES));
            
            doc.add(new StringField(QueryUtils.VER_FIELD_NAME, getBytes(row,2), Field.Store.YES));
            
            doc.add(new StringField(FIELD_TABLE, table, Field.Store.YES));
            
            long time = System.currentTimeMillis();
            long expires = Long.MAX_VALUE;            
           
            
            doc.add(new StringField(LUCENE_FIELD_MODIFIED,
                    DateTools.timeToString(time, DateTools.Resolution.SECOND),
                    Field.Store.YES));
            
            doc.add(new StoredField(EXPIRATION_TIME_FIELD_NAME, expires));
                    
            for (int index : indexColumns) {
                String columnName = columns[index];
                if(row[index]==null){
                	continue;
                }
                // index string array
                if(row[index].getClass().isArray()){
                	
                	if(row[index] instanceof String[]){
                		String[] terms = (String[])row[index];
                		for(int j=0;j<terms.length;j++){
                			doc.add(new TextField(columnName, terms[j], storeText));    
                		}
                	}
                	else if(row[index] instanceof Object[]){
                		Object[] terms = (Object[])row[index];
                		for(int j=0;j<terms.length;j++){
                			doc.add(new TextField(columnName, terms[j].toString(), storeText));    
                		}
                	}
                }
                else{
	                String data = asString(row[index], columnTypes[index]);
	                // column names that start with _
	                // must be escaped to avoid conflicts
	                // with internal field names (_DATA, _QUERY, _modified)
	                
	                doc.add(new TextField(columnName, data, storeText));       
                }
            }
           
            try {
                indexAccess.writer.addDocument(doc);
                if (commitIndex) {
                	indexAccess.commitIndex();
                }
                else{
                	indexAccess.increment();
                }
            } catch (IOException e) {
                throw convertException(e);
            }
        }

        /**
         * Delete a row from the index.
         *
         * @param row the row
         * @param commitIndex whether to commit the changes to the Lucene index
         */
        protected void delete(Object[] row, boolean commitIndex) throws SQLException {
        	BytesRef query = getBytes(row,0);
            try {
                Term term = new Term(FIELD_KEY, query);
                indexAccess.writer.deleteDocuments(term);
                if (commitIndex) {
                	indexAccess.commitIndex();
                }
            } catch (IOException e) {
                throw convertException(e);
            }
        }
        
        private BytesRef getBytes(Object[] row,int i){    
           BytesRef keyByteRef = null;
           if(row[i] instanceof GridH2ValueCacheObject){
        	   GridH2ValueCacheObject _key = (GridH2ValueCacheObject)row[i];
        	   keyByteRef = new BytesRef(_key.getBytesNoCopy());
           }
           else if(row[i] instanceof BinaryObjectImpl){
        	   BinaryObjectImpl _key = (BinaryObjectImpl)row[i];
        	   keyByteRef = new BytesRef(_key.array());
           }
           else{           
        	   keyByteRef = new BytesRef(row[i].toString());
           }
           return keyByteRef;
        }
       
    }

    /**
     * Throw a SQLException with the given message.
     *
     * @param message the message
     * @return never returns normally
     * @throws SQLException the exception
     */
    protected static SQLException throwException(String message)
            throws SQLException {
        throw new SQLException(message, "FULLTEXT");
    }
}

