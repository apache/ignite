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
package org.apache.ignite.internal.commandline.cache;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheArguments {
    /** Command. */
    private CacheCommand cmd;

    /** Caches. */
    private Set<String> caches;

    /** Partition id. */
    private int partId;

    /** Regex. */
    private String regex;

    /** Node id. */
    private UUID nodeId;

    /** Min queue size. */
    private int minQueueSize;

    /** Max print. */
    private int maxPrint;

    /** validate_indexes 'checkFirst' argument */
    private int checkFirst = -1;

    /** validate_indexes 'checkThrough' argument */
    private int checkThrough = -1;

    /** Cache view command. */
    private @Nullable VisorViewCacheCmd cacheCmd;

    /** Calculate partition hash and print into standard output. */
    private boolean dump;

    /** Skip zeros partitions. */
    private boolean skipZeros;

    /** Additional user attributes in result. Set of attribute names whose values will be searched in ClusterNode.attributes(). */
    private Set<String> userAttributes;

    /**
     * @return Command.
     */
    public CacheCommand command() {
        return cmd;
    }

    /**
     * @return Cache view command.
     */
    @Nullable public VisorViewCacheCmd cacheCommand() {
        return cacheCmd;
    }

    /**
     * @param cmd Cache view command.
     */
    public void cacheCommand(VisorViewCacheCmd cmd) {
        this.cacheCmd = cmd;
    }

    /**
     * @param cmd New command.
     */
    public void command(CacheCommand cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Caches.
     */
    public Set<String> caches() {
        return caches;
    }

    /**
     * @param caches New caches.
     */
    public void caches(Set<String> caches) {
        this.caches = caches;
    }

    /**
     * @return Partition id.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @param partId New partition id.
     */
    public void partitionId(int partId) {
        this.partId = partId;
    }

    /**
     * @return Regex.
     */
    public String regex() {
        return regex;
    }

    /**
     * @param regex New regex.
     */
    public void regex(String regex) {
        this.regex = regex;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId New node id.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Min queue size.
     */
    public int minQueueSize() {
        return minQueueSize;
    }

    /**
     * @param minQueueSize New min queue size.
     */
    public void minQueueSize(int minQueueSize) {
        this.minQueueSize = minQueueSize;
    }

    /**
     * @return Max print.
     */
    public int maxPrint() {
        return maxPrint;
    }

    /**
     * @param maxPrint New max print.
     */
    public void maxPrint(int maxPrint) {
        this.maxPrint = maxPrint;
    }

    /**
     * @return Max number of entries to be checked.
     */
    public int checkFirst() {
        return checkFirst;
    }

    /**
     * @param checkFirst Max number of entries to be checked.
     */
    public void checkFirst(int checkFirst) {
        this.checkFirst = checkFirst;
    }

    /**
     * @return Number of entries to check through.
     */
    public int checkThrough() {
        return checkThrough;
    }

    /**
     * @param checkThrough Number of entries to check through.
     */
    public void checkThrough(int checkThrough) {
        this.checkThrough = checkThrough;
    }

    /**
     * @return Calculate partition hash and print into standard output.
     */
    public boolean dump() {
        return dump;
    }

    /**
     * @param dump Calculate partition hash and print into standard output.
     */
    public void dump(boolean dump) {
        this.dump = dump;
    }

    /**
     * @return Skip zeros partitions(size == 0) in result.
     */
    public boolean isSkipZeros() {
        return skipZeros;
    }

    /**
     * @param skipZeros Skip zeros partitions.
     */
    public void skipZeros(boolean skipZeros) {
        this.skipZeros = skipZeros;
    }

    /**
     * @return Additional user attributes in result. Set of attribute names whose values will be searched in ClusterNode.attributes().
     */
    public Set<String> getUserAttributes() {
        return userAttributes;
    }

    /**
     * @param userAttrs New additional user attributes in result.
     */
    public void setUserAttributes(Set<String> userAttrs) {
        userAttributes = userAttrs;
    }
}
