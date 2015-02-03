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

package org.apache.ignite.configuration;

import org.apache.ignite.cache.*;

/**
 *
 */
public class IgniteCollectionConfiguration {
    /** Cache name. */
    private String cacheName;

    /** */
    private boolean collocated;

    /**
     * @return {@code True} if all items within the same collection will be collocated on the same node.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param collocated If {@code true} then all items within the same collection will be collocated on the same node.
     *      Otherwise elements of the same set maybe be cached on different nodes. This parameter works only
     *      collections stored in {@link CacheMode#PARTITIONED} cache.
     */
    public void setCollocated(boolean collocated) {
        this.collocated = collocated;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }
}
