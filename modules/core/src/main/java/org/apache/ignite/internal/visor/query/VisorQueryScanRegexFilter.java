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

package org.apache.ignite.internal.visor.query;

import java.util.regex.Pattern;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Filter scan results by specified substring in string presentation of key or value.
 */
public class VisorQueryScanRegexFilter implements IgniteBiPredicate<Object, Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Regex pattern to search data. */
    private final Pattern ptrn;

    /**
     * Create filter instance.
     *
     * @param caseSensitive Case sensitive flag.
     * @param regex Regex search flag.
     * @param ptrn String to search in string presentation of key or value.
     */
    public VisorQueryScanRegexFilter(boolean caseSensitive, boolean regex, String ptrn) {
        int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;

        this.ptrn = Pattern.compile(regex ? ptrn : ".*?" + Pattern.quote(ptrn) + ".*?", flags);
    }

    /**
     * Check that key or value contains specified string.
     *
     * @param key Key object.
     * @param val Value object.
     * @return {@code true} when string presentation of key or value contain specified string.
     */
    @Override public boolean apply(Object key, Object val) {
        String k = key instanceof BinaryObject ? VisorQueryUtils.binaryToString((BinaryObject)key) : key.toString();
        String v = val instanceof BinaryObject ? VisorQueryUtils.binaryToString((BinaryObject)val) : val.toString();

        return ptrn.matcher(k).find() || ptrn.matcher(v).find();
    }
}
