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

package org.apache.ignite.internal.processors.resource;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Wrapper for data where resource should be injected.
 * Bean contains {@link Method} and {@link Annotation} for that method.
 */
class GridResourceMethod {
    /** */
    static final GridResourceMethod[] EMPTY_ARRAY = new GridResourceMethod[0];

    /** Method which used to inject resource. */
    private final Method mtd;

    /** Resource annotation. */
    private final Annotation ann;

    /**
     * Creates new bean.
     *
     * @param mtd Method which used to inject resource.
     * @param ann Resource annotation.
     */
    GridResourceMethod(Method mtd, Annotation ann) {
        assert mtd != null;
        assert ann != null;

        this.mtd = mtd;
        this.ann = ann;

        mtd.setAccessible(true);
    }

    /**
     * Gets class method object.
     *
     * @return Class method.
     */
    public Method getMethod() {
        return mtd;
    }

    /**
     * Gets annotation for class method object.
     *
     * @return Method annotation.
     */
    public Annotation getAnnotation() {
        return ann;
    }

    /**
     * @param c Closure.
     */
    public static GridResourceMethod[] toArray(Collection<GridResourceMethod> c) {
        if (c.isEmpty())
            return EMPTY_ARRAY;

        return c.toArray(new GridResourceMethod[c.size()]);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceMethod.class, this);
    }
}