/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
