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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@ArgumentGroup(value = {"cacheNames", "groupNames"}, optional = false)
public class CacheScheduleIndexesRebuildCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** --cache-names parameter format. */
    private static final String CACHE_NAMES_FORMAT = "cacheName[index1,...indexN],cacheName2,cacheName3[index1]";

    /** */
    @Argument(
        description = "(Optional) Specify node for indexes rebuild. If not specified, schedules rebuild on all nodes",
        example = "nodeId",
        optional = true)
    private UUID nodeId;

    /** */
    @Argument(description = "Comma-separated list of cache names with optionally specified indexes. " +
        "If indexes are not specified then all indexes of the cache will be scheduled for the rebuild operation. " +
        "Can be used simultaneously with cache group names",
        example = "cacheName[index1,...indexN],cacheName2,cacheName3[index1]")
    private String cacheNames;

    /** */
    @Argument(description = "Comma-separated list of cache group names for which indexes should be scheduled for the "
        + "rebuild. Can be used simultaneously with cache names",
        example = "groupName1,groupName2,...groupNameN")
    private String[] groupNames;

    /** Cache name -> indexes. */
    private Map<String, Set<String>> cacheToIndexes;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        U.writeString(out, cacheNames);
        U.writeArray(out, groupNames);
        U.writeMap(out, cacheToIndexes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        cacheNames = U.readString(in);
        groupNames = U.readArray(in, String.class);
        cacheToIndexes = U.readMap(in);
    }

    /** */
    private void parse() {
        cacheToIndexes = new HashMap<>();

        Pattern cacheNamesPattern = Pattern.compile("([^,\\[\\]]+)(\\[(.*?)])?");
        Matcher matcher = cacheNamesPattern.matcher(cacheNames);

        boolean found = false;

        while (matcher.find()) {
            found = true;

            String cacheName = matcher.group(1);
            boolean specifiedIndexes = matcher.group(2) != null;
            String commaSeparatedIndexes = matcher.group(3);

            if (!specifiedIndexes) {
                cacheToIndexes.put(cacheName, Collections.emptySet());

                continue;
            }

            if (F.isEmpty(commaSeparatedIndexes)) {
                throw new IllegalArgumentException("Square brackets must contain comma-separated indexes or not be used "
                    + "at all.");
            }

            Set<String> indexes = Arrays.stream(commaSeparatedIndexes.split(",")).collect(Collectors.toSet());
            cacheToIndexes.put(cacheName, indexes);
        }

        if (!found)
            throw new IllegalArgumentException("Wrong format for --cache-names, should be: " + CACHE_NAMES_FORMAT);
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
    public String cacheNames() {
        return cacheNames;
    }

    /** */
    public void cacheNames(String cacheNames) {
        this.cacheNames = cacheNames;
        parse();
    }

    /** */
    public String[] groupNames() {
        return groupNames;
    }

    /** */
    public void groupNames(String[] groupNames) {
        this.groupNames = groupNames;
    }

    /** */
    public Map<String, Set<String>> cacheToIndexes() {
        return cacheToIndexes;
    }

    /** */
    public void cacheToIndexes(Map<String, Set<String>> cacheToIndexes) {
        this.cacheToIndexes = cacheToIndexes;
    }
}
