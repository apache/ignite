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

package org.apache.ignite.internal.cache.query.index.sorted;

/**
 * List of settings that affects key types of index keys.
 */
public class IndexKeyTypeSettings {
    /** Whether inlining POJO keys as hash is supported. */
    private final boolean inlineObjHash;

    /** Whether inlining of POJO keys is supported. */
    private final boolean inlineObjSupported;

    /** Whether optimized algoruthm of String comparison is used. */
    private final boolean useStrOptimizedCompare;

    /** */
    public IndexKeyTypeSettings(boolean inlineObjHash, boolean inlineObjSupported, boolean useStrOptimizedCompare) {
        this.inlineObjHash = inlineObjHash;
        this.inlineObjSupported = inlineObjSupported;
        this.useStrOptimizedCompare = useStrOptimizedCompare;
    }

    /** */
    public boolean inlineObjHash() {
        return inlineObjHash;
    }

    /** */
    public boolean inlineObjSupported() {
        return inlineObjSupported;
    }

    /** */
    public boolean useStringOptimizedCompare() {
        return useStrOptimizedCompare;
    }
}
