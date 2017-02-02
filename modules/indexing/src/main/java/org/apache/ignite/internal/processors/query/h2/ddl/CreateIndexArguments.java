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

package org.apache.ignite.internal.processors.query.h2.ddl;

import org.apache.ignite.cache.QueryIndex;

/**
 * Arguments for {@code CREATE INDEX}.
 */
public class CreateIndexArguments implements DdlOperationArguments {
    /** Cache name. */
    public final String cacheName;

    /** Index. */
    public final QueryIndex idx;

    /** Ignore operation if index exists. */
    public final boolean ifNotExists;

    /**
     * @param cacheName Cache name.
     * @param idx Index params.
     * @param ifNotExists Ignore operation  if index exists.
     */
    public CreateIndexArguments(String cacheName, QueryIndex idx, boolean ifNotExists) {
        this.cacheName = cacheName;
        this.idx = idx;
        this.ifNotExists = ifNotExists;
    }
}
