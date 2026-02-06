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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.Positional;

/** */
public class CacheValidateIndexesCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(value = 0)
    @Positional
    @Argument(example = "cacheName1,...,cacheNameN", optional = true)
    String value;

    /** */
    @Order(value = 1)
    @Positional
    @Argument(example = "nodeId", optional = true)
    String value2;

    /** */
    @Order(value = 2)
    String[] caches;

    /** */
    @Order(value = 3)
    UUID[] nodeIds;

    /** */
    @Order(value = 4)
    @Argument(example = "N", description = "validate only the first N keys", optional = true)
    int checkFirst = -1;

    /** */
    @Order(value = 5)
    @Argument(example = "K", description = "validate every Kth key", optional = true)
    int checkThrough = -1;

    /** */
    @Order(value = 6)
    @Argument(description = "check the CRC-sum of pages stored on disk", optional = true)
    boolean checkCrc;

    /** */
    @Order(value = 7)
    @Argument(description = "check that index size and cache size are the same", optional = true)
    boolean checkSizes;

    /** */
    private static void ensurePositive(int numVal, String arg) {
        if (numVal <= 0)
            throw new IllegalArgumentException("Value for '" + arg + "' property should be positive.");
    }

    /** */
    private void parse(String value) {
        try {
            nodeIds = CommandUtils.parseVal(value, UUID[].class);

            return;
        }
        catch (IllegalArgumentException ignored) {
            //No-op.
        }

        caches = CommandUtils.parseVal(value, String[].class);
    }

    /** */
    public String value2() {
        return value2;
    }

    /** */
    public void value2(String value2) {
        this.value2 = value2;

        parse(value2);
    }

    /** */
    public String value() {
        return value;
    }

    /** */
    public void value(String value) {
        this.value = value;

        parse(value);
    }

    /** */
    public UUID[] nodeIds() {
        return nodeIds;
    }

    /** */
    public void nodeIds(UUID[] nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** */
    public String[] caches() {
        return caches;
    }

    /** */
    public void caches(String[] caches) {
        this.caches = caches;
    }

    /** */
    public int checkFirst() {
        return checkFirst;
    }

    /** */
    public void checkFirst(int checkFirst) {
        if (this.checkFirst == checkFirst)
            return;

        ensurePositive(checkFirst, "--check-first");

        this.checkFirst = checkFirst;
    }

    /** */
    public int checkThrough() {
        return checkThrough;
    }

    /** */
    public void checkThrough(int checkThrough) {
        if (this.checkThrough == checkThrough)
            return;

        ensurePositive(checkThrough, "--check-through");

        this.checkThrough = checkThrough;
    }

    /** */
    public boolean checkCrc() {
        return checkCrc;
    }

    /** */
    public void checkCrc(boolean checkCrc) {
        this.checkCrc = checkCrc;
    }

    /** */
    public boolean checkSizes() {
        return checkSizes;
    }

    /** */
    public void checkSizes(boolean checkSizes) {
        this.checkSizes = checkSizes;
    }
}
