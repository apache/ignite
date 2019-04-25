/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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