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

import java.io.*;
import java.util.*;

/**
 * Arguments for {@link VisorQueryTask}.
 */
public class VisorQueryArg implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID in case of local cache. */
    private final UUID locCacheNodeId;

    /** Cache name for query. */
    private final String cacheName;

    /** Query text. */
    private final String qryTxt;

    /** Result batch size. */
    private final Integer pageSize;

    /**
     * @param locCacheNodeId Node ID in case of local cache or {@code null} otherwise.
     * @param cacheName Cache name for query.
     * @param qryTxt Query text.
     * @param pageSize Result batch size.
     */
    public VisorQueryArg(UUID locCacheNodeId, String cacheName, String qryTxt, Integer pageSize) {
        this.locCacheNodeId = locCacheNodeId;
        this.cacheName = cacheName;
        this.qryTxt = qryTxt;
        this.pageSize = pageSize;
    }

    /**
     * @return Node ID in case of local cache or {@code null} otherwise.
     */
    public UUID localCacheNodeId() {
        return locCacheNodeId;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Query txt.
     */
    public String queryTxt() {
        return qryTxt;
    }

    /**
     * @return Page size.
     */
    public Integer pageSize() {
        return pageSize;
    }
}
