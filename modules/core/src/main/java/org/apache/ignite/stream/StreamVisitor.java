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

package org.apache.ignite.stream;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteBiInClosure;

/**
 * Convenience adapter to visit every key-value tuple in the stream. Note, that the visitor
 * does not update the cache. If the tuple needs to be stored in the cache,
 * then {@code cache.put(...)} should be called explicitly.
 */
public abstract class  StreamVisitor<K, V> implements StreamReceiver<K, V>, IgniteBiInClosure<IgniteCache<K, V>, Map.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException {
        for (Map.Entry<K, V> entry : entries)
            apply(cache, entry);
    }

    /**
     * Creates a new visitor based on instance of {@link IgniteBiInClosure}.
     *
     * @param c Closure.
     * @return Stream visitor.
     */
    public static <K, V> StreamVisitor<K, V> from(final IgniteBiInClosure<IgniteCache<K, V>, Map.Entry<K, V>> c) {
        return new StreamVisitor<K, V>() {
            @Override public void apply(IgniteCache<K, V> cache, Map.Entry<K, V> entry) {
                c.apply(cache, entry);
            }
        };
    }
}