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

package org.apache.ignite.internal.management.cache.scan;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;

/** */
public class CacheScanCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** Default entries limit. */
    private static final int DFLT_LIMIT = 1_000;

    /** */
    @Positional
    @Argument(example = "cacheName")
    String cacheName;

    /** */
    @Argument(description = "Pluggable output format. 'default', 'table' exists by default", example = "table", optional = true)
    String outputFormat;

    /** */
    @Argument(description = "limit count of entries to scan (" + DFLT_LIMIT + " by default)", example = "N", optional = true)
    int limit = DFLT_LIMIT;

    /** */
    public String outputFormat() {
        return outputFormat;
    }

    /** */
    public void outputFormat(String format) {
        this.outputFormat = format;
    }

    /** */
    public String cacheName() {
        return cacheName;
    }

    /** */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public int limit() {
        return limit;
    }

    /** */
    public void limit(int limit) {
        this.limit = limit;
    }
}
