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

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * List of settings that affects key types of index keys.
 */
public class IndexKeyTypeSettings implements Message {
    /** Whether inlining POJO keys as hash is supported. */
    @Order(0)
    boolean inlineObjHash = true;

    /** Whether inlining of POJO keys is supported. */
    @Order(1)
    boolean inlineObjSupported = true;

    /** Whether optimized algorithm of String comparison is used. */
    @Order(2)
    boolean strOptimizedCompare = true;

    /** Whether use unsigned bytes for storing byte arrays. */
    @Order(3)
    boolean binaryUnsigned = true;

    /** {@inheritDoc} */
    @Override public short directType() {
        return 19;
    }

    /** */
    public boolean inlineObjectHash() {
        return inlineObjHash;
    }

    /** */
    public IndexKeyTypeSettings inlineObjectHash(boolean inlineObjHash) {
        this.inlineObjHash = inlineObjHash;

        return this;
    }

    /** */
    public boolean inlineObjectSupported() {
        return inlineObjSupported;
    }

    /** */
    public IndexKeyTypeSettings inlineObjectSupported(boolean inlineObjSupported) {
        this.inlineObjSupported = inlineObjSupported;

        return this;
    }

    /** */
    public boolean stringOptimizedCompare() {
        return strOptimizedCompare;
    }

    /** */
    public IndexKeyTypeSettings stringOptimizedCompare(boolean strOptimizedCompare) {
        this.strOptimizedCompare = strOptimizedCompare;

        return this;
    }

    /** */
    public boolean binaryUnsigned() {
        return binaryUnsigned;
    }

    /** */
    public IndexKeyTypeSettings binaryUnsigned(boolean binaryUnsigned) {
        this.binaryUnsigned = binaryUnsigned;

        return this;
    }
}
