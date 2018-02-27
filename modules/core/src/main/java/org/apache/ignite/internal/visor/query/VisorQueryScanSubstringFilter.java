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

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Filter scan results by specified substring in string presentation of key or value.
 */
public class VisorQueryScanSubstringFilter implements IgniteBiPredicate<Object, Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Case sensitive flag. */
    private final boolean caseSensitive;

    /** String to search in string presentation of key or value. */
    private final String ptrn;

    /**
     * Create filter instance.
     *
     * @param caseSensitive Case sensitive flag.
     * @param ptrn String to search in string presentation of key or value.
     */
    public VisorQueryScanSubstringFilter(boolean caseSensitive, String ptrn) {
        this.caseSensitive = caseSensitive;

        this.ptrn = caseSensitive ? ptrn : ptrn.toUpperCase();
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

        if (caseSensitive)
            return k.contains(ptrn) || v.contains(ptrn);

        return k.toUpperCase().contains(ptrn) || v.toUpperCase().contains(ptrn);
    }
}
