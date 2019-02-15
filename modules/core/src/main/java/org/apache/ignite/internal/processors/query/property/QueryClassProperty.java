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

package org.apache.ignite.internal.processors.query.property;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Description of type property.
 */
public class QueryClassProperty implements GridQueryProperty {
    /** */
    private final QueryPropertyAccessor accessor;

    /** */
    private final boolean key;

    /** */
    private QueryClassProperty parent;

    /** */
    private final String name;

    /** */
    private final CacheObjectContext coCtx;

    /** */
    private final boolean notNull;

    /**
     * Constructor.
     *
     * @param accessor Way of accessing the property.
     * @param key {@code true} if key property, {@code false} otherwise.
     * @param name Property name.
     * @param notNull {@code true} if null value is not allowed.
     * @param coCtx Cache Object Context.
     */
    public QueryClassProperty(QueryPropertyAccessor accessor, boolean key, String name,
        boolean notNull, @Nullable CacheObjectContext coCtx) {
        this.accessor = accessor;

        this.key = key;

        this.name = !F.isEmpty(name) ? name : accessor.getPropertyName();

        this.notNull = notNull;

        this.coCtx = coCtx;
    }

    /** {@inheritDoc} */
    @Override public Object value(Object key, Object val) throws IgniteCheckedException {
        Object x = unwrap(this.key ? key : val);

        if (parent != null)
            x = parent.value(key, val);

        if (x == null)
            return null;

        return accessor.getValue(x);
    }

    /** {@inheritDoc} */
    @Override public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException {
        Object x = unwrap(this.key ? key : val);

        if (parent != null)
            x = parent.value(key, val);

        if (x == null)
            return;

        accessor.setValue(x, propVal);
    }

    /** {@inheritDoc} */
    @Override public boolean key() {
        return key;
    }

    /**
     * Unwraps cache object, if needed.
     *
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    private Object unwrap(Object o) {
        return coCtx == null ? o : o instanceof CacheObject ? ((CacheObject)o).value(coCtx, false) : o;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Class<?> type() {
        return accessor.getType();
    }

    /**
     * @param parent Parent property if this is embeddable element.
     */
    public void parent(QueryClassProperty parent) {
        this.parent = parent;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryClassProperty.class, this);
    }

    /** {@inheritDoc} */
    @Override public GridQueryProperty parent() {
        return parent;
    }

    /** {@inheritDoc} */
    @Override public boolean notNull() {
        return notNull;
    }

    /** {@inheritDoc} */
    @Override public Object defaultValue() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return -1;
    }
}
