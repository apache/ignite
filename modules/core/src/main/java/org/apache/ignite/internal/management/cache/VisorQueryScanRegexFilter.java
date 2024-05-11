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
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
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
        String k = key instanceof BinaryObject ? binaryToString((BinaryObject)key) : key.toString();
        String v = val instanceof BinaryObject ? binaryToString((BinaryObject)val) : val.toString();

        return ptrn.matcher(k).find() || ptrn.matcher(v).find();
    }
    

    /**
     * Convert Binary object to string.
     *
     * @param obj Binary object.
     * @return String representation of Binary object.
     */
    public static String binaryToString(BinaryObject obj) {
        int hash = obj.hashCode();

        if (obj instanceof BinaryObjectEx) {
            BinaryObjectEx objEx = (BinaryObjectEx)obj;

            BinaryType meta;

            try {
                meta = ((BinaryObjectEx)obj).rawType();
            }
            catch (BinaryObjectException ignore) {
                meta = null;
            }

            if (meta != null) {
                if (meta.isEnum()) {
                    try {
                        return obj.deserialize().toString();
                    }
                    catch (BinaryObjectException ignore) {
                        // NO-op.
                    }
                }

                SB buf = new SB(meta.typeName());

                if (meta.fieldNames() != null) {
                    buf.a(" [hash=").a(hash);

                    for (String name : meta.fieldNames()) {
                        Object val = objEx.field(name);

                        buf.a(", ").a(name).a('=').a(val);
                    }

                    buf.a(']');

                    return buf.toString();
                }
            }
        }

        return S.toString(obj.getClass().getSimpleName(),
            "hash", hash, false,
            "typeId", obj.type().typeId(), true);
    }


}
