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

/**
 * Wrapper for class which should be injected.
 * Bean contains {@link Class} and {@link Annotation} for that resource.
 */
class GridResourceClass {
    /** Class where resource should be injected. */
    private final Class<?> cls;

    /** Resource annotation class. */
    private final Class<? extends Annotation> annCls;

    /**
     * Creates new bean.
     *
     * @param cls Class where resource should be injected.
     * @param annCls Resource annotation class.
     */
    GridResourceClass(Class<?> cls, Class<? extends Annotation> annCls) {
        assert cls != null;
        assert annCls != null;

        this.cls = cls;
        this.annCls = annCls;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridResourceClass that = (GridResourceClass)o;

        if (annCls != null ? !annCls.equals(that.annCls) : that.annCls != null)
            return false;
        if (cls != null ? !cls.equals(that.cls) : that.cls != null)
            return false;

        return true;
    }

    @Override public int hashCode() {
        int result = cls != null ? cls.hashCode() : 0;
        result = 31 * result + (annCls != null ? annCls.hashCode() : 0);
        return result;
    }

    /**
     * Gets class field object.
     *
     * @return Class field.
     */
    public Class<?> getCls() {
        return cls;
    }

    /**
     * Gets annotation for class field object.
     *
     * @return Annotation class to inject.
     */
    public Class<? extends Annotation> getAnnCls() {
        return annCls;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResourceClass.class, this);
    }
}
