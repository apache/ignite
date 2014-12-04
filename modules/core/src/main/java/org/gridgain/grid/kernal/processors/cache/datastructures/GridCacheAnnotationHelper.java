/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Utility class for getting annotated values from classes.
 * Contains local cache of annotated methods and fields by classes for best performance.
 */
public class GridCacheAnnotationHelper<A extends Annotation> {
    /** Number of entries to keep in annotation cache. */
    private static final int DFLT_CLASS_CACHE_SIZE = 1000;

    /** Field cache. */
    private final GridBoundedLinkedHashMap<Class<?>, List<Field>> fieldCache;

    /** Method cache. */
    private final GridBoundedLinkedHashMap<Class<?>, List<Method>> mtdCache;

    /** Annotation class. */
    private final Class<A> annCls;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Constructor.
     *
     * @param annCls Annotation class.
     */
    public GridCacheAnnotationHelper(Class<A> annCls) {
        this(annCls, DFLT_CLASS_CACHE_SIZE);
    }

    /**
     * Constructor.
     *
     * @param annCls Annotation class.
     * @param capacity Capacity of local caches.
     */
    public GridCacheAnnotationHelper(Class<A> annCls, int capacity) {
        assert annCls != null : "Annotated class mustn't be null.";
        assert capacity > 0 : "Capacity must be more then zero.";

        this.annCls = annCls;

        fieldCache = new GridBoundedLinkedHashMap<>(capacity);

        mtdCache = new GridBoundedLinkedHashMap<>(capacity);
    }

    /**
     * Returns annotated value.
     *
     * @param target Object to find a value in.
     * @return Value of annotated field or method.
     * @throws GridException If failed to find.
     */
    public Object annotatedValue(Object target) throws GridException {
        IgniteBiTuple<Object, Boolean> res = annotatedValue(target, new HashSet<>(), false);

        assert res != null;

        return res.get1();
    }

    /**
     * Returns annotated value.
     *
     * @param target Object to find a value in.
     * @param visited Set of visited objects to avoid cycling.
     * @param annFound Flag indicating if value has already been found.
     * @return Value of annotated field or method.
     * @throws GridException If failed to find.
     */
    private IgniteBiTuple<Object, Boolean> annotatedValue(Object target, Set<Object> visited, boolean annFound)
        throws GridException {
        assert target != null;

        // To avoid infinite recursion.
        if (visited.contains(target))
            return F.t(null, annFound);

        visited.add(target);

        Object val = null;

        for (Class<?> cls = target.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass()) {
            // Fields.
            for (Field f : fieldsWithAnnotation(cls)) {
                f.setAccessible(true);

                Object fieldVal;

                try {
                    fieldVal = f.get(target);
                }
                catch (IllegalAccessException e) {
                    throw new GridException("Failed to get annotated field value [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName()+']', e);
                }

                if (needsRecursion(f)) {
                    if (fieldVal != null) {
                        // Recursion.
                        IgniteBiTuple<Object, Boolean> tup = annotatedValue(fieldVal, visited, annFound);

                        if (!annFound && tup.get2())
                            // Update value only if annotation was found in recursive call.
                            val = tup.get1();

                        annFound = tup.get2();
                    }
                }
                else {
                    if (annFound)
                        throw new GridException("Multiple annotations has been found [cls=" + cls.getName() +
                            ", ann=" + annCls.getSimpleName() + ']');

                    val = fieldVal;

                    annFound = true;
                }
            }

            // Methods.
            for (Method m : methodsWithAnnotation(cls)) {
                if (annFound)
                    throw new GridException("Multiple annotations has been found [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName() + ']');

                m.setAccessible(true);

                try {
                    val = m.invoke(target);
                }
                catch (Exception e) {
                    throw new GridException("Failed to get annotated method value [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName()+']', e);
                }

                annFound = true;
            }
        }

        return F.t(val, annFound);
    }

    /**
     * @param f Field.
     * @return {@code true} if recursive inspection is required.
     */
    private boolean needsRecursion(Field f) {
        assert f != null;

        // Need to inspect anonymous classes, callable and runnable instances.
        return f.getName().startsWith("this$") || f.getName().startsWith("val$") ||
            Callable.class.isAssignableFrom(f.getType()) || Runnable.class.isAssignableFrom(f.getType());
    }

    /**
     * Gets all entries from the specified class or its super-classes that have
     * been annotated with annotation provided.
     *
     * @param cls Class in which search for fields.
     * @return Set of entries with given annotations.
     */
    private Iterable<Field> fieldsWithAnnotation(Class<?> cls) {
        synchronized (mux) {
            List<Field> fields = fieldCache.get(cls);
            if (fields == null) {
                fields = new ArrayList<>();

                for (Field field : cls.getDeclaredFields()) {
                    Annotation ann = field.getAnnotation(annCls);

                    if (ann != null || needsRecursion(field))
                        fields.add(field);
                }

                if (!fields.isEmpty())
                    fieldCache.put(cls, fields);
            }

            return fields;
        }
    }

    /**
     * Gets set of methods with given annotation.
     *
     * @param cls Class in which search for methods.
     * @return Set of methods with given annotations.
     */
    private Iterable<Method> methodsWithAnnotation(Class<?> cls) {
        synchronized (mux) {
            List<Method> mtds = mtdCache.get(cls);

            if (mtds == null) {
                mtds = new ArrayList<>();

                for (Method mtd : cls.getDeclaredMethods()) {
                    Annotation ann = mtd.getAnnotation(annCls);

                    if (ann != null)
                        mtds.add(mtd);
                }

                if (!mtds.isEmpty())
                    mtdCache.put(cls, mtds);
            }

            return mtds;
        }
    }
}
