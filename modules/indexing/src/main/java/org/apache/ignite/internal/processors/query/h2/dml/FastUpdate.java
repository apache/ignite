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

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Arguments for fast, query-less UPDATE or DELETE - key and, optionally, value and new value.
 */
public final class FastUpdate {
    /** Operand to compute key. */
    private final DmlArgument keyArg;

    /** Operand to compute value. */
    private final DmlArgument valArg;

    /** Operand to compute new value. */
    private final DmlArgument newValArg;

    /**
     * Create fast update instance.
     *
     * @param key Key element.
     * @param val Value element.
     * @param newVal New value element (if any)
     * @return Fast update.
     */
    public static FastUpdate create(GridSqlElement key, GridSqlElement val, @Nullable GridSqlElement newVal) {
        DmlArgument keyArg = DmlArguments.create(key);
        DmlArgument valArg = DmlArguments.create(val);
        DmlArgument newValArg = DmlArguments.create(newVal);

        return new FastUpdate(keyArg, valArg, newValArg);
    }

    /**
     * Constructor.
     *
     * @param keyArg Key argument.
     * @param valArg Value argument.
     * @param newValArg New value argument.
     */
    private FastUpdate(DmlArgument keyArg, DmlArgument valArg, DmlArgument newValArg) {
        this.keyArg = keyArg;
        this.valArg = valArg;
        this.newValArg = newValArg;
    }

    /**
     * Perform single cache operation based on given args.
     *
     * @param cache Cache.
     * @param args Query parameters.
     * @return 1 if an item was affected, 0 otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked"})
    public UpdateResult execute(GridCacheAdapter cache, Object[] args) throws IgniteCheckedException {
        Object key = keyArg.get(args);

        assert key != null;

        Object val = valArg.get(args);
        Object newVal = newValArg.get(args);

        boolean res;

        if (newVal != null) {
            // Update.
            if (val != null)
                res = cache.replace(key, val, newVal);
            else
                res = cache.replace(key, newVal);
        }
        else {
            // Delete.
            if (val != null)
                res = cache.remove(key, val);
            else
                res = cache.remove(key);
        }

        return res ? UpdateResult.ONE : UpdateResult.ZERO;
    }

    /**
     *
     * @param args Query Parameters.
     * @return Key and value.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteBiTuple getRow(Object[] args) throws IgniteCheckedException {
        Object key = keyArg.get(args);

        assert key != null;

        Object newVal = newValArg.get(args);

        return new IgniteBiTuple(key, newVal);
    }
}
