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
     * @return Query type resolver class name.
     */
    public String typeResolver() {
        return typeRslvr;
    }

    /**
     * @return {@code true} if primitive keys should be indexed.
     */
    public boolean indexPrimitiveKey() {
        return idxPrimitiveKey;
    }

    /**
     * @return {@code true} if primitive values should be indexed.
     */
    public boolean indexPrimitiveValue() {
        return idxPrimitiveVal;
    }

    /**
     * @return {@code true} if SQL engine should try to convert values to their respective SQL types.
     */
    public boolean indexFixedTyping() {
        return idxFixedTyping;
    }

    /**
     * @return {@code true} if SQL engine generate SQL statements with escaped names.
     */
    public boolean escapeAll() {
        return escapeAll;
    }
}
