/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.util.typedef.internal.*;
import java.lang.annotation.*;
import java.lang.reflect.*;

/**
 * Wrapper for data where resource should be injected.
 * Bean contains {@link Method} and {@link Annotation} for that method.
 *
 * @author @java.author
 * @version @java.version
 */
class GridResourceMethod {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceMethod.class, this);
    }
}
