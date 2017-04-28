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

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClasses;

/**
 * Data transfer object for cache query configuration data.
 */
public class VisorQueryConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<String> sqlFuncClss;

    /** */
    private long longQryWarnTimeout;

    /** */
    private boolean sqlEscapeAll;

    /** */
    private List<String> indexedTypes;

    /** */
    private String sqlSchema;

    /**
     * Default constructor.
     */
    public VisorQueryConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object with cache query configuration data.
     *
     * @param ccfg Cache configuration.
     */
    public VisorQueryConfiguration(CacheConfiguration ccfg) {
        sqlFuncClss = compactClasses(ccfg.getSqlFunctionClasses());
        longQryWarnTimeout = ccfg.getLongQueryWarningTimeout();
        sqlEscapeAll = ccfg.isSqlEscapeAll();
        indexedTypes = compactClasses(ccfg.getIndexedTypes());
        sqlSchema = ccfg.getSqlSchema();
    }

    /**
     * @return Classes names with SQL functions.
     */
    public List<String> getSqlFunctionClasses() {
        return sqlFuncClss;
    }

    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    public long getLongQueryWarningTimeout() {
        return longQryWarnTimeout;
    }

    /**
     * @return {@code true} if SQL engine generate SQL statements with escaped names.
     */
    public boolean isSqlEscapeAll() {
        return sqlEscapeAll;
    }

    /**
     * @return Array of key and value classes names to be indexed.
     */
    public List<String> getIndexedTypes() {
        return indexedTypes;
    }

    /**
     * @return Schema name, which is used by SQL engine for SQL statements generation.
     */
    public String getSqlSchema() {
        return sqlSchema;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, sqlFuncClss);
        out.writeLong(longQryWarnTimeout);
        out.writeBoolean(sqlEscapeAll);
        U.writeCollection(out, indexedTypes);
        U.writeString(out, sqlSchema);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        sqlFuncClss = U.readList(in);
        longQryWarnTimeout = in.readLong();
        sqlEscapeAll = in.readBoolean();
        indexedTypes = U.readList(in);
        sqlSchema = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryConfiguration.class, this);
    }
}
