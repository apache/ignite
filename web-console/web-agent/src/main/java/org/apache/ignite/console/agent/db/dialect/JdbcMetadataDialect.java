

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.console.agent.db.DbColumn;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.agent.db.Dialect;
import org.apache.ignite.console.agent.utils.DBMetadataUtils;

/**
 * Metadata dialect that uses standard JDBC for reading metadata.
 */
public class JdbcMetadataDialect extends DatabaseMetadataDialect {
    /** */
    static final String[] TABLES_ONLY = {"TABLE"};
    
    /** */
    static final String[] VIEW_ONLY = {"VIEW"};

    /** */
    static final String[] TABLES_AND_VIEWS = {"TABLE", "VIEW"};

    /** Schema catalog index. */
    static final int TBL_CATALOG_IDX = 1;

    /** Schema name index. */
    static final int TBL_SCHEMA_IDX = 2;

    /** Table name index. */
    static final int TBL_NAME_IDX = 3;

    /** Primary key column name index. */
    static final int PK_COL_NAME_IDX = 4;

    /** Column name index. */
    static final int COL_NAME_IDX = 4;

    /** Column data type index. */
    static final int COL_DATA_TYPE_IDX = 5;

    /** Column type name index. */
    static final int COL_TYPE_NAME_IDX = 6;

    /** Column nullable index. */
    static final int COL_NULLABLE_IDX = 11;

    /** Index name index. */
    static final int IDX_NAME_IDX = 6;

    /** Index column name index. */
    static final int IDX_COL_NAME_IDX = 9;

    /** Index column descend index. */
    static final int IDX_ASC_OR_DESC_IDX = 10;
    
    protected DBMetadataUtils metaUtil;
    
    protected final Connection conn;
    
    protected boolean hasTableComment = true;
    
    protected boolean hasColumnComment = true;
    
    public JdbcMetadataDialect(Connection conn,Dialect dialect) {
    	this.conn = conn;
    	this.metaUtil = new DBMetadataUtils(conn,dialect);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> schemas(Connection conn, boolean importSamples) throws SQLException {
        Collection<String> schemas = new ArrayList<>();

        ResultSet rs = getSchemas(conn);

        Set<String> sys = systemSchemas();
        Set<String> samples = sampleSchemas();

        while(rs.next()) {
            String schema = rs.getString(1);

            // Skip system schemas.
            if (sys.contains(schema))
                continue;

            // Skip sample schemas.
            if (!importSamples && samples.contains(schema))
                continue;

            schemas.add(schema);
        }

        return schemas;
    }

    /**
     * @return If {@code true} use catalogs for table division.
     */
    protected boolean useCatalog() {
        return false;
    }

    /**
     * @return If {@code true} use schemas for table division.
     */
    protected boolean useSchema() {
        return true;
    }
    

    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, List<String> schemas, boolean tblsOnly)
        throws SQLException {
    	return tables(conn,schemas,"%", tblsOnly? TABLES_ONLY:TABLES_AND_VIEWS);
    }

    /** {@inheritDoc} */
    public Collection<DbTable> tables(Connection conn, List<String> schemas, String tableNamePattern, String[] tblsOnly)
        throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();

        Set<String> sys = systemSchemas();

        Collection<String> unsignedTypes = unsignedTypes(dbMeta);

        if (schemas.isEmpty())
            schemas.add(null);

        Collection<DbTable> tbls = new ArrayList<>();

        for (String toSchema: schemas) {
            try (ResultSet tblsRs = dbMeta.getTables(useCatalog() ? toSchema : null, useSchema() ? toSchema : null, tableNamePattern, tblsOnly)) {
                while (tblsRs.next()) {
                    String tblCatalog = tblsRs.getString(TBL_CATALOG_IDX);
                    String tblSchema = tblsRs.getString(TBL_SCHEMA_IDX);
                    String tblName = tblsRs.getString(TBL_NAME_IDX);

                    // In case of MySql we should use catalog.
                    String schema = tblSchema != null ? tblSchema : tblCatalog;

                    // Skip system schemas.
                    if (sys.contains(schema))
                        continue;
                    
                    String tableComment = tableCommentColumn(tblsRs,toSchema,tblName);

                    Set<String> pkCols = new LinkedHashSet<>();
                    
                    pkCols.addAll(getPrimaryKeyDefines(conn, tblCatalog, tblSchema, tblName));

                    Map.Entry<String, Set<String>> uniqueIdxAsPk = null;

                    // If PK not found, trying to use first UNIQUE index as key.
                    if (pkCols.isEmpty()) {
                        Map<String, Set<String>> uniqueIdxs = new LinkedHashMap<>();

                        try (ResultSet idxRs = dbMeta.getIndexInfo(tblCatalog, tblSchema, tblName, true, true)) {
                            while (idxRs.next()) {
                                String idxName = idxRs.getString(IDX_NAME_IDX);
                                String colName = idxRs.getString(IDX_COL_NAME_IDX);

                                if (idxName == null || colName == null)
                                    continue;

                                Set<String> idxCols = uniqueIdxs.computeIfAbsent(idxName, k -> new LinkedHashSet<>());

                                idxCols.add(colName);
                            }
                        }

                        uniqueIdxAsPk = uniqueIndexAsPk(uniqueIdxs);

                        if (uniqueIdxAsPk != null)
                            pkCols.addAll(uniqueIdxAsPk.getValue());
                    }

                    Collection<DbColumn> cols = new ArrayList<>();

                    try (ResultSet colsRs = dbMeta.getColumns(tblCatalog, tblSchema, tblName, null)) {
                        while (colsRs.next()) {
                            String colName = colsRs.getString(COL_NAME_IDX);
                            String colType = colsRs.getString(COL_TYPE_NAME_IDX);
                            DbColumn col = new DbColumn(
                                    colName,
                                    colsRs.getInt(COL_DATA_TYPE_IDX),
                                    colType,
                                    pkCols.contains(colName),
                                    colsRs.getInt(COL_NULLABLE_IDX) == DatabaseMetaData.columnNullable,
                                    unsignedTypes.contains(colType));
                            
                            col.setComment(columnCommentColumn(colsRs,toSchema,tblName,colName));

                            cols.add(col);
                        }
                    }

                    String uniqueIdxAsPkName = uniqueIdxAsPk != null ? uniqueIdxAsPk.getKey() : null;

                    Map<String, QueryIndex> idxs = new LinkedHashMap<>();

                    try (ResultSet idxRs = dbMeta.getIndexInfo(tblCatalog, tblSchema, tblName, false, true)) {
                        while (idxRs.next()) {
                            String idxName = idxRs.getString(IDX_NAME_IDX);
                            String colName = idxRs.getString(IDX_COL_NAME_IDX);

                            // Skip {@code null} names and unique index used as PK.
                            if (idxName == null || colName == null || idxName.equals(uniqueIdxAsPkName))
                                continue;

                            QueryIndex idx = idxs.get(idxName);

                            if (idx == null) {
                                idx = index(idxName);

                                idxs.put(idxName, idx);
                            }

                            String askOrDesc = idxRs.getString(IDX_ASC_OR_DESC_IDX);

                            Boolean asc = askOrDesc == null || "A".equals(askOrDesc);

                            idx.getFields().put(colName, asc);
                        }
                    }

                    // Remove index that is equals to primary key.
                    if (!pkCols.isEmpty()) {
                        for (Map.Entry<String, QueryIndex> entry : idxs.entrySet()) {
                            QueryIndex idx = entry.getValue();

                            if (pkCols.equals(idx.getFields().keySet())) {
                                idxs.remove(entry.getKey());
                                break;
                            }
                        }
                    }

                    tbls.add(table(schema, tblName, tableComment, cols, idxs.values()));
                }
            }
            
            if(!this.hasTableComment) {
	            Map<String,String> tableComments = this.metaUtil.getIntrospector().getTableComments(useCatalog() ? toSchema : null, useSchema() ? toSchema : null);
	            if(tableComments!=null) {
	                for(DbTable tab: tbls) {
	                	String remarks = tableComments.get(tab.getTable());
	                	if(remarks!=null) {
	                		tab.setComment(remarks);
	                	}
	                }
	            }
            }
            
            if(!this.hasColumnComment) {
	            Map<String, Map<String, String>> tableColumnComments = this.metaUtil.getIntrospector().getColumnComments(useCatalog() ? toSchema : null, useSchema() ? toSchema : null);
	            
	            if(tableColumnComments!=null) {
	                for(DbTable tab: tbls) {
	                	Map<String, String> remarks = tableColumnComments.get(tab.getTable());
	                	if(remarks!=null) {
	                		for(DbColumn col: tab.getColumns()) {
	                			String remark = remarks.get(col.getName());
	                			if(remark!=null) {
	                				col.setComment(remark);
	                			}
	                		}
	                		
	                	}
	                }
	            }
            }
        }

        return tbls;
    }
    
    
	protected List<String> getPrimaryKeyDefines(Connection connection, String tblCatalog, String tblSchema, String tblName) throws SQLException {
    	DatabaseMetaData dbMeta = conn.getMetaData();
    	List<String> pkCols = new ArrayList<>();

        try (ResultSet pkRs = dbMeta.getPrimaryKeys(tblCatalog, tblSchema, tblName)) {
            while (pkRs.next())
                pkCols.add(pkRs.getString(PK_COL_NAME_IDX));
        }
        return pkCols;
	}
    
    /**获取表注解*/
    protected String tableCommentColumn(ResultSet colsRs,String toSchema,String tableName) {        
    	try {
    		if(hasTableComment)
    			return colsRs.getString("REMARKS");
		} catch (SQLException e) {
			hasTableComment = false;			
		}
    	return null;
    }
    
    /**获取字段注解*/
    protected String columnCommentColumn(ResultSet colsRs,String toSchema,String tableName,String colName) {        
        try {
        	if(hasColumnComment)
        		return colsRs.getString("REMARKS");
		} catch (SQLException e) {
			hasColumnComment = false;			
		}
        return null;
    }

}
