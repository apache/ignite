/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.console.agent.db.DbColumn;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.db.VisorQueryIndex;
import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cache.support.SimpleCacheManager;


/**
 * Base class for database metadata dialect.
 */
public abstract class DatabaseMetadataDialect {	
	
	static ConcurrentMapCacheManager cacheMng = new ConcurrentMapCacheManager();
	
	static Cache cache = null;
	
	static long lastUpdated = System.currentTimeMillis();
	
    /**
     * Gets schemas from database.
     *
     * @param conn Database connection.
     * @param importSamples If {@code true} include sample schemas.
     * @return Collection of schema descriptors.
     * @throws SQLException If failed to get schemas.
     */
    public abstract Collection<String> schemas(Connection conn, boolean importSamples) throws SQLException;

    /**
     * Get DB schema metadata.
     *
     * @param conn Database connection.
     * @return Result set with schemas information.
     */
    protected ResultSet getSchemas(Connection conn) throws SQLException {
        return conn.getMetaData().getSchemas();
    }

    /**
     * Gets tables from database.
     *
     * @param conn Database connection.
     * @param schemas Collection of schema names to load.
     * @param tblsOnly If {@code true} then gets only tables otherwise gets tables and views.
     * @return Collection of table descriptors.
     * @throws SQLException If failed to get tables.
     */
    public abstract Collection<DbTable> tables(Connection conn, List<String> schemas, boolean tblsOnly)
        throws SQLException;

    /**
     * @return Collection of database system schemas.
     */
    public Set<String> systemSchemas() {
        return Collections.singleton("INFORMATION_SCHEMA");
    }

    /**
     * @return Collection of sample schemas.
     */
    public Set<String> sampleSchemas() {
        return Collections.emptySet();
    }

    /**
     * @return Collection of unsigned type names.
     * @throws SQLException If failed to get unsigned type names.
     */
    public Set<String> unsignedTypes(DatabaseMetaData dbMeta) throws SQLException {
        return Collections.emptySet();
    }

    /**
     * Create table descriptor.
     *
     * @param schema Schema name.
     * @param tbl Table name.
     * @param cols Table columns.
     * @param idxs Table indexes.
     * @return New {@code DbTable} instance.
     */
    protected DbTable table(String schema, String tbl, String comment,Collection<DbColumn> cols, Collection<QueryIndex>idxs) {
        Collection<VisorQueryIndex> res = new ArrayList<>(idxs.size());

        for (QueryIndex idx : idxs)
            res.add(new VisorQueryIndex(idx));

        return new DbTable(schema, tbl, comment, cols, res);
    }

    /**
     * Create index descriptor.
     *
     * @param idxName Index name.
     * @return New initialized {@code QueryIndex} instance.
     */
    protected QueryIndex index(String idxName) {
        QueryIndex idx = new QueryIndex();

        idx.setName(idxName);
        idx.setIndexType(QueryIndexType.SORTED);
        idx.setFields(new LinkedHashMap<>());

        return idx;
    }

    /**
     * Select first shortest index.
     *
     * @param uniqueIdxs Unique indexes with columns.
     * @return Unique index that could be used instead of primary key.
     */
    protected Map.Entry<String, Set<String>> uniqueIndexAsPk(Map<String, Set<String>> uniqueIdxs) {
        Map.Entry<String, Set<String>> uniqueIdxAsPk = null;

        for (Map.Entry<String, Set<String>> uniqueIdx : uniqueIdxs.entrySet()) {
            if (uniqueIdxAsPk == null || uniqueIdxAsPk.getValue().size() > uniqueIdx.getValue().size())
                uniqueIdxAsPk = uniqueIdx;
        }

        return uniqueIdxAsPk;
    }
    
    public Collection<DbTable> cachedTables(Connection conn, List<String> schemas, boolean tblsOnly) throws SQLException {
    	String key = ""+conn.getCatalog()+schemas.toString()+tblsOnly;
    	if(cache == null) {    		
    		cache = cacheMng.getCache("tableMetas");
    	}
    	Collection<DbTable> tables = cache.get(key,Collection.class);
    	if(tables==null || tables.isEmpty()) {
    		tables = this.tables(conn, schemas, tblsOnly);
    		cache.put(key, tables);
    		return tables;
    	}
    	else {
    		long now = System.currentTimeMillis();
    		if(now - lastUpdated > 10* 60*1000) {
    			lastUpdated = now;
    			cache.evict(key);
    		}
    	}
    	return tables;
    }
}
