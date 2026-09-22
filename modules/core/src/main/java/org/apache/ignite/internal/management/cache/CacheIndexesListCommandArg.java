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

package org.apache.ignite.internal.management.cache;

import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;

/** */
public class CacheIndexesListCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Argument(
        example = "nodeId",
        optional = true,
        description = "Specify node for job execution. If not specified explicitly, node will be chosen by grid")
    UUID nodeId;

    /** */
    @Order(1)
    @Argument(
        example = "grpRegExp",
        optional = true,
        description = "Regular expression allowing filtering by cache group name")
    String groupName;

    /** */
    @Order(2)
    @Argument(
        example = "cacheRegExp",
        optional = true,
        description = "Regular expression allowing filtering by cache name")
    String cacheName;

    /** */
    @Order(3)
    @Argument(
        example = "idxNameRegExp",
        optional = true,
        description = "Regular expression allowing filtering by index name")
    String indexName;

    /**
     * @param regex Regex to validate
     * @return {@code True} if {@code regex} syntax is valid. {@code False} otherwise.
     */
    private boolean validateRegEx(String name, String regex) {
        try {
            Pattern.compile(regex);

            return true;
        }
        catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid " + name + " name regex: " + regex);
        }
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public String groupName() {
        return groupName;
    }

    /** */
    public void groupName(String groupName) {
        validateRegEx("group", groupName);
        this.groupName = groupName;
    }

    /** */
    public String cacheName() {
        return cacheName;
    }

    /** */
    public void cacheName(String cacheName) {
        validateRegEx("cache", cacheName);
        this.cacheName = cacheName;
    }

    /** */
    public String indexName() {
        return indexName;
    }

    /** */
    public void indexName(String indexName) {
        validateRegEx("index", indexName);
        this.indexName = indexName;
    }
}
