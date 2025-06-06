

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.console.agent.db.DbColumn;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.agent.db.Dialect;


/**
 * DB2 specific metadata dialect.
 */
public class HiveSQLMetadataDialect extends JdbcMetadataDialect {

	/** Type name index. */
    private static final int TYPE_NAME_IDX = 1;
    private static final String TBLPROPERTIES = "TBLPROPERTIES";
    private static final String ROW_KEY = "holodesk.rowkey";
	
	public HiveSQLMetadataDialect(Connection conn) {
		super(conn, Dialect.GENERIC);		
	}
	

    /** {@inheritDoc} */
    @Override protected boolean useCatalog() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSchema() {
        return true;
    }
    
    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("system", "information_schema"));
    }
    
    @Override public Set<String> unsignedTypes(DatabaseMetaData dbMeta) throws SQLException {
        Set<String> unsignedTypes = new HashSet<>();

        try (ResultSet typeRs = dbMeta.getTypeInfo()) {
            while (typeRs.next()) {
                String typeName = typeRs.getString(TYPE_NAME_IDX);

                if (typeName.toLowerCase().contains("unsigned"))
                    unsignedTypes.add(typeName);
            }
        }

        return unsignedTypes;
    }
    
    /** {@inheritDoc} */
    @Override
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
                    

                    // If PK not found, trying to use first UNIQUE index as key.
                    if (pkCols.isEmpty()) {
                    	List<String> rowKey = getPrimaryKeyDefinesFromRowKey(conn, tblCatalog, tblSchema, tblName);
                    	pkCols.addAll(rowKey);
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
                                    pkCols.contains(colName.toLowerCase()),
                                    colsRs.getInt(COL_NULLABLE_IDX) == DatabaseMetaData.columnNullable,
                                    unsignedTypes.contains(colType));
                            
                            col.setComment(columnCommentColumn(colsRs,toSchema,tblName,colName));

                            cols.add(col);
                        }
                    }                    

                    tbls.add(table(schema, tblName, tableComment, cols, List.of()));
                }
            }
        }

        return tbls;
    }
    
    
    
	protected List<String> getPrimaryKeyDefinesFromRowKey(Connection connection, String catalog, String schema, String table) {
		
		List<String> primaryKeyList = new ArrayList<>();
		
		try {
			String sql = "show create table "+schema+"."+table+";";
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			
			String primaryKey = null;			
			StringBuilder tableDesc = new StringBuilder();
			while (resultSet.next()) {
				tableDesc.append(resultSet.getString(1));
				tableDesc.append("\n");
			}
			resultSet.close();
			statement.close();
			
			if(tableDesc.length()>0) {
				int tblproperties_index = tableDesc.indexOf(TBLPROPERTIES);
				if(tblproperties_index>0) {
					String tblproperties = tableDesc.substring(tblproperties_index+TBLPROPERTIES.length());
					
					String[] props = tblproperties.trim().split("\n");
					for(String prop: props) {
						prop = prop.trim();
						if(prop.startsWith("'"+ROW_KEY+"'")) {
							primaryKey = prop.substring(ROW_KEY.length()+3).toLowerCase();
							String [] keysList = primaryKey.replaceAll("\'|\\)|\\s", "").split(",");
							primaryKeyList.addAll(Arrays.asList(keysList));
							break;
						}
					}
				
				}
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return primaryKeyList;
	}
    
}
