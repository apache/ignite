// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.lang.annotation.*;
import java.util.concurrent.*;

/**
 * Caches class loaders for classes.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridAnnotationsCache {
    /** Annotation cache. */
    private static final ConcurrentMap<Class<?>,
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>>> anns =
        new ConcurrentHashMap8<>();

    /**
     * @param cls Class.
     * @param annCls Annotation class.
     * @return Annotation (or {@code null}).
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> clsAnns = anns.get(cls);

        if (clsAnns == null) {
            ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> old = anns.putIfAbsent(cls,
                clsAnns = new ConcurrentHashMap8<>());

            if (old != null)
                clsAnns = old;
        }

        GridTuple<T> ann = (GridTuple<T>)clsAnns.get(annCls);

        if (ann == null) {
            ann = F.t(U.getAnnotation(cls, annCls));

            clsAnns.putIfAbsent(annCls, (GridTuple<Annotation>)ann);
        }

        return ann.get();
    }


    /**
     * @param ldr Undeployed class loader.
     */
    public static void onUndeployed(ClassLoader ldr) {
        assert ldr != null;

        for (Class<?> cls : anns.keySet()) {
            if (ldr.equals(cls.getClassLoader()))
                anns.remove(cls);
        }
    }

    /**
     * Ensure singleton.
     */
    private GridAnnotationsCache() {
        // No-op.
    }
}
