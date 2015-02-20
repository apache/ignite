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

import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for cache query configuration data.
 */
public class VisorCacheQueryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type resolver class name. */
    private String typeRslvr;

    /** {@code true} if primitive keys should be indexed. */
    private boolean idxPrimitiveKey;

    /** {@code true} if primitive values should be indexed. */
    private boolean idxPrimitiveVal;

    /** {@code true} if SQL engine should try to convert values to their respective SQL types. */
    private boolean idxFixedTyping;

    /** {@code true} if SQL engine should generate SQL statements with escaped names. */
    private boolean escapeAll;

    /**
     * @param qccfg Cache query configuration.
     * @return Fill data transfer object with cache query configuration data.
     */
    public static VisorCacheQueryConfiguration from(CacheQueryConfiguration qccfg) {
        VisorCacheQueryConfiguration cfg = null;

        if (qccfg != null) {
            cfg = new VisorCacheQueryConfiguration();

            QueryTypeResolver rslvr = qccfg.getTypeResolver();

            if (rslvr != null)
                cfg.typeResolver(U.compact(rslvr.getClass().getName()));

            cfg.indexPrimitiveKey(qccfg.isIndexPrimitiveKey());
            cfg.indexPrimitiveValue(qccfg.isIndexPrimitiveValue());
            cfg.indexFixedTyping(qccfg.isIndexFixedTyping());
            cfg.escapeAll(qccfg.isEscapeAll());
        }

        return cfg;
    }

    /**
     * @return Query type resolver class name.
     */
    public String typeResolver() {
        return typeRslvr;
    }

    /**
     * @param typeRslvr Query type resolver class name.
     */
    public void typeResolver(String typeRslvr) {
        this.typeRslvr = typeRslvr;
    }

    /**
     * @return {@code true} if primitive keys should be indexed.
     */
    public boolean indexPrimitiveKey() {
        return idxPrimitiveKey;
    }

    /**
     * @param idxPrimitiveKey {@code true} if primitive keys should be indexed.
     */
    public void indexPrimitiveKey(boolean idxPrimitiveKey) {
        this.idxPrimitiveKey = idxPrimitiveKey;
    }

    /**
     * @return {@code true} if primitive values should be indexed.
     */
    public boolean indexPrimitiveValue() {
        return idxPrimitiveVal;
    }

    /**
     * @param idxPrimitiveVal {@code true} if primitive values should be indexed.
     */
    public void indexPrimitiveValue(boolean idxPrimitiveVal) {
        this.idxPrimitiveVal = idxPrimitiveVal;
    }

    /**
     * @return {@code true} if SQL engine should try to convert values to their respective SQL types.
     */
    public boolean indexFixedTyping() {
        return idxFixedTyping;
    }

    /**
     * @param idxFixedTyping {@code true} if SQL engine should try to convert values to their respective SQL types.
     */
    public void indexFixedTyping(boolean idxFixedTyping) {
        this.idxFixedTyping = idxFixedTyping;
    }

    /**
     * @return {@code true} if SQL engine generate SQL statements with escaped names.
     */
    public boolean escapeAll() {
        return escapeAll;
    }

    /**
     * @param escapeAll {@code true} if SQL engine should generate SQL statements with escaped names.
     */
    public void escapeAll(boolean escapeAll) {
        this.escapeAll = escapeAll;
    }
}
