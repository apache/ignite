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

package org.apache.ignite.stream.kafka.connect;

/**
 * Sink configuration strings.
 */
public class IgniteSinkConstants {
    /** Ignite configuration file path. */
    public static final String CACHE_CFG_PATH = "igniteCfg";

    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** Flag to enable overwriting existing values in cache. */
    public static final String CACHE_ALLOW_OVERWRITE = "cacheAllowOverwrite";

    /** Size of per-node buffer before data is sent to remote node. */
    public static final String CACHE_PER_NODE_DATA_SIZE = "cachePerNodeDataSize";

    /** Maximum number of parallel stream operations per node. */
    public static final String CACHE_PER_NODE_PAR_OPS = "cachePerNodeParOps";

    /** Class to transform the entry before feeding into cache. */
    public static final String SINGLE_TUPLE_EXTRACTOR_CLASS = "singleTupleExtractorCls";
}
