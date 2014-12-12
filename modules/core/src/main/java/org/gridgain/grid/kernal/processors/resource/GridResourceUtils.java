/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;

import java.lang.reflect.*;
import java.util.concurrent.*;

/**
 * Collection of utility methods used in package for classes reflection.
 */
final class GridResourceUtils {
    /**
     * Ensure singleton.
     */
    private GridResourceUtils() {
        // No-op.
    }

    /**
     * Sets the field represented by this {@code field} object on the
     * specified object argument {@code target} to the specified new value {@code rsrc}.
     *
     * @param field Field where resource should be injected.
     * @param target Target object.
     * @param rsrc Resource object which should be injected in target object field.
     * @throws IgniteCheckedException Thrown if unable to inject resource.
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    static void inject(Field field, Object target, Object rsrc) throws IgniteCheckedException {
        if (rsrc != null && !field.getType().isAssignableFrom(rsrc.getClass()))
            throw new IgniteCheckedException("Resource field is not assignable from the resource: " + rsrc.getClass());

        try {
            // Override default Java access check.
            field.setAccessible(true);

            field.set(target, rsrc);
        }
        catch (SecurityException | ExceptionInInitializerError | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to inject resource [field=" + field.getName() +
                ", target=" + target + ", rsrc=" + rsrc + ']', e);
        }
    }

    /**
     * Invokes the underlying method {@code mtd} represented by this
     * {@link Method} object, on the specified object {@code target}
     * with the specified parameter object {@code rsrc}.
     *
     * @param mtd Method which should be invoked to inject resource.
     * @param target Target object.
     * @param rsrc Resource object which should be injected.
     * @throws IgniteCheckedException Thrown if unable to inject resource.
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    static void inject(Method mtd, Object target, Object rsrc) throws IgniteCheckedException {
        if (mtd.getParameterTypes().length != 1 ||
            (rsrc != null && !mtd.getParameterTypes()[0].isAssignableFrom(rsrc.getClass()))) {
            throw new IgniteCheckedException("Setter does not have single parameter of required type [type=" +
                rsrc.getClass().getName() + ", setter=" + mtd + ']');
        }

        try {
            mtd.setAccessible(true);

            mtd.invoke(target, rsrc);
        }
        catch (IllegalAccessException | ExceptionInInitializerError | InvocationTargetException e) {
            throw new IgniteCheckedException("Failed to inject resource [method=" + mtd.getName() +
                ", target=" + target + ", rsrc=" + rsrc + ']', e);
        }
    }

    /**
     * Checks if specified field requires recursive inspection to find resource annotations.
     *
     * @param f Field.
     * @return {@code true} if requires, {@code false} if doesn't.
     */
    static boolean mayRequireResources(Field f) {
        assert f != null;

        // Need to inspect anonymous classes, callable and runnable instances.
        return f.getName().startsWith("this$") || f.getName().startsWith("val$") ||
            Callable.class.isAssignableFrom(f.getType()) || Runnable.class.isAssignableFrom(f.getType());
    }
}
