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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Positional;

import static java.lang.String.format;

/** */
public class CacheIdleVerifyCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(optional = true, example = "cacheName1,...,cacheNameN")
    String[] caches;

    /** */
    @Argument(optional = true)
    boolean skipZeros;

    /** */
    @Argument(description = "check the CRC-sum of pages stored on disk before verifying data consistency " +
        "in partitions between primary and backup nodes", optional = true)
    boolean checkCrc;

    /** */
    @Argument(optional = true, example = "cacheName1,...,cacheNameN")
    String[] excludeCaches;

    /** */
    @Argument(optional = true, description = "Type of cache(s)")
    @EnumDescription(
        names = {
            "DEFAULT",
            "SYSTEM",
            "PERSISTENT",
            "NOT_PERSISTENT",
            "USER",
            "ALL"
        },
        descriptions = {
            "Default - user only, or all caches specified by name",
            "System",
            "Persistent",
            "Not persistent",
            "User",
            "All"
        }
    )
    CacheFilterEnum cacheFilter = CacheFilterEnum.DEFAULT;

    /**
     * @param string To validate that given name is valed regex.
     */
    private void validateRegexes(String[] string) {
        if (string == null)
            return;

        for (String s : string) {
            try {
                Pattern.compile(s);
            }
            catch (PatternSyntaxException e) {
                throw new IgniteException(format("Invalid cache name regexp '%s': %s", s, e.getMessage()));
            }
        }
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
    public boolean skipZeros() {
        return skipZeros;
    }

    /** */
    public void skipZeros(boolean skipZeros) {
        this.skipZeros = skipZeros;
    }

    /** */
    public String[] excludeCaches() {
        return excludeCaches;
    }

    /** */
    public void excludeCaches(String[] excludeCaches) {
        this.excludeCaches = excludeCaches;

        validateRegexes(excludeCaches);
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
    public CacheFilterEnum cacheFilter() {
        return cacheFilter;
    }

    /** */
    public void cacheFilter(CacheFilterEnum cacheFilter) {
        this.cacheFilter = cacheFilter;
    }
}
