/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Data transfer object for cache query configuration data.
 */
public class VisorCacheQueryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String[] sqlFuncClss;

    /** */
    private long longQryWarnTimeout;

    /** */
    private boolean sqlEscapeAll;

    /** */
    private String[] indexedTypes;

    /** */
    private int sqlOnheapRowCacheSize;

    /**
     * @param clss Classes to compact.
     */
    private static String[] compactClasses(Class<?>[] clss) {
        if (clss == null)
            return null;

        int len = clss.length;

        String[] res = new String[len];

        for (int i = 0; i < len; i++)
            res[i] = U.compact(clss[i].getName());

        return res;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Fill data transfer object with cache query configuration data.
     */
    public static VisorCacheQueryConfiguration from(CacheConfiguration ccfg) {
        VisorCacheQueryConfiguration cfg = new VisorCacheQueryConfiguration();

        cfg.sqlFuncClss = compactClasses(ccfg.getSqlFunctionClasses());
        cfg.longQryWarnTimeout = ccfg.getLongQueryWarningTimeout();
        cfg.sqlEscapeAll = ccfg.isSqlEscapeAll();
        cfg.indexedTypes = compactClasses(ccfg.getIndexedTypes());
        cfg.sqlOnheapRowCacheSize = ccfg.getSqlOnheapRowCacheSize();

        return cfg;
    }

    /**
     * @return Classes names with SQL functions.
     */
    public String[] sqlFunctionClasses() {
        return sqlFuncClss;
    }

    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    public long longQueryWarningTimeout() {
        return longQryWarnTimeout;
    }

    /**
     * @return {@code true} if SQL engine generate SQL statements with escaped names.
     */
    public boolean sqlEscapeAll() {
        return sqlEscapeAll;
    }

    /**
     * @return Array of key and value classes names to be indexed.
     */
    public String[] indexedTypes() {
        return indexedTypes;
    }

    /**
     * @return Number of SQL rows which will be cached onheap to avoid deserialization on each SQL index access.
     */
    public int sqlOnheapRowCacheSize() {
        return sqlOnheapRowCacheSize;
    }
}