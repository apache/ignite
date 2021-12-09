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

package org.apache.ignite.internal.configuration.direct;

import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Key path node, used to represent key and its meta information.
 */
public class KeyPathNode {
    /** Key in path. */
    @IgniteToStringInclude
    public final String key;

    /** Flag that indicates whether the key is a part of named list or not. */
    @IgniteToStringInclude
    public final boolean namedListEntry;

    /** Flag that indicates whether the key is a part of named list and represents a name rather than internal id. */
    @IgniteToStringInclude
    public final boolean unresolvedName;

    /**
     * Constructor for regular keys.
     *
     * @param key Key.
     */
    public KeyPathNode(String key) {
        this.key = key;
        this.unresolvedName = false;
        this.namedListEntry = false;
    }

    /**
     * Constructor for named list elements.
     *
     * @param key Key.
     * @param unresolvedName {@code true} if key is unresolved name, {@code false} if it's an internal id.
     */
    public KeyPathNode(String key, boolean unresolvedName) {
        this.key = key;
        this.unresolvedName = unresolvedName;
        this.namedListEntry = true;
    }

    @Override
    public String toString() {
        return S.toString(KeyPathNode.class, this);
    }
}
