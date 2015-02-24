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

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for query configuration data.
 */
public class VisorQueryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Classes names with methods annotated by {@link QuerySqlFunction}. */
    private String[] idxCustomFuncClss;

    /** Optional search paths consisting of space names to search SQL schema objects. */
    private String[] searchPath;

    /** Script path to be ran against H2 database after opening. */
    private String initScriptPath;

    /** Maximum amount of memory available to off-heap storage. */
    private long maxOffHeapMemory = -1;

    /** Query execution time threshold. */
    private long longQryExecTimeout;

    /** If {@code true}, SPI will print SQL execution plan for long queries. */
    private boolean longQryExplain;

    /** The flag indicating that serializer for H2 database will be set to Ignite's marshaller. */
    private boolean useOptimizedSerializer;

    /**
     * @param qcfg Query configuration.
     * @return Fill data transfer object with query configuration data.
     */
    public static VisorQueryConfiguration from(QueryConfiguration qcfg) {
        VisorQueryConfiguration c = null;

        if (qcfg != null) {
            c = new VisorQueryConfiguration();

            Class<?>[] clss = qcfg.getIndexCustomFunctionClasses();

            int sz = clss != null ? clss.length : 0;

            String[] strClss = new String[sz];

            for (int i = 0; i < sz; i++)
                strClss[i] = U.compact(clss[i].getName());

            c.indexCustomFunctionClasses(strClss);
            c.searchPath(qcfg.getSearchPath());
            c.initialScriptPath(qcfg.getInitialScriptPath());
            c.maxOffHeapMemory(qcfg.getMaxOffHeapMemory());
            c.longQueryExecutionTimeout(qcfg.getLongQueryExecutionTimeout());
            c.longQueryExplain(qcfg.isLongQueryExplain());
            c.useOptimizedSerializer(qcfg.isUseOptimizedSerializer());
        }

        return c;
    }

    /**
     * @return Classes with methods annotated by {@link QuerySqlFunction}.
     */
    public String[] indexCustomFunctionClasses() {
        return idxCustomFuncClss;
    }

    /**
     * @param idxCustomFuncClss Classes with methods annotated by {@link QuerySqlFunction}.
     */
    public void indexCustomFunctionClasses(String[] idxCustomFuncClss) {
        this.idxCustomFuncClss = idxCustomFuncClss;
    }

    /**
     * @return Optional search path consisting of space names to search SQL schema objects.
     */
    public String[] searchPath() {
        return searchPath;
    }

    /**
     * @param searchPath Optional search path consisting of space names to search SQL schema objects.
     */
    public void searchPath(String[] searchPath) {
        this.searchPath = searchPath;
    }

    /**
     * @return Script path to be ran against H2 database after opening.
     */
    public String initialScriptPath() {
        return initScriptPath;
    }

    /**
     * @param initScriptPath Script path to be ran against H2 database after opening.
     */
    public void initialScriptPath(String initScriptPath) {
        this.initScriptPath = initScriptPath;
    }

    /**
     * @return Maximum amount of memory available to off-heap storage.
     */
    public long maxOffHeapMemory() {
        return maxOffHeapMemory;
    }

    /**
     * @param maxOffHeapMemory Maximum amount of memory available to off-heap storage.
     */
    public void maxOffHeapMemory(long maxOffHeapMemory) {
        this.maxOffHeapMemory = maxOffHeapMemory;
    }

    /**
     * @return Query execution time threshold.
     */
    public long longQueryExecutionTimeout() {
        return longQryExecTimeout;
    }

    /**
     * @param longQryExecTimeout Query execution time threshold.
     */
    public void longQueryExecutionTimeout(long longQryExecTimeout) {
        this.longQryExecTimeout = longQryExecTimeout;
    }

    /**
     * @return If {@code true}, SPI will print SQL execution plan for long queries.
     */
    public boolean longQryExplain() {
        return longQryExplain;
    }

    /**
     * @param longQryExplain If {@code true}, SPI will print SQL execution plan for long queries.
     */
    public void longQueryExplain(boolean longQryExplain) {
        this.longQryExplain = longQryExplain;
    }

    /**
     * @return The flag indicating that serializer for H2 database will be set to Ignite's marshaller.
     */
    public boolean useOptimizedSerializer() {
        return useOptimizedSerializer;
    }

    /**
     * @param useOptimizedSerializer The flag indicating that serializer for H2 database will be set to Ignite's
     * marshaller.
     */
    public void useOptimizedSerializer(boolean useOptimizedSerializer) {
        this.useOptimizedSerializer = useOptimizedSerializer;
    }
}
