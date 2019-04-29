/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.h2.engine.SysProperties;

/**
 * Unsafe memory unmapper.
 *
 * @see SysProperties#NIO_CLEANER_HACK
 */
public final class MemoryUnmapper {

    private static final boolean ENABLED;

    private static final Object UNSAFE;

    private static final Method INVOKE_CLEANER;

    static {
        boolean enabled = SysProperties.NIO_CLEANER_HACK;
        Object unsafe = null;
        Method invokeCleaner = null;
        if (enabled) {
            try {
                Class<?> clazz = Class.forName("sun.misc.Unsafe");
                Field field = clazz.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = field.get(null);
                // This method exists only on Java 9 and later versions
                invokeCleaner = clazz.getMethod("invokeCleaner", ByteBuffer.class);
            } catch (ReflectiveOperationException e) {
                // Java 7 or 8
                unsafe = null;
                // invokeCleaner can be only null here
            } catch (Throwable e) {
                // Should be a SecurityException, but catch everything to be
                // safe
                enabled = false;
                unsafe = null;
                // invokeCleaner can be only null here
            }
        }
        ENABLED = enabled;
        UNSAFE = unsafe;
        INVOKE_CLEANER = invokeCleaner;
    }

    /**
     * Tries to unmap memory for the specified byte buffer using Java internals
     * in unsafe way if {@link SysProperties#NIO_CLEANER_HACK} is enabled and
     * access is not denied by a security manager.
     *
     * @param buffer
     *            mapped byte buffer
     * @return whether operation was successful
     */
    public static boolean unmap(ByteBuffer buffer) {
        if (!ENABLED) {
            return false;
        }
        try {
            if (INVOKE_CLEANER != null) {
                // Java 9 or later
                INVOKE_CLEANER.invoke(UNSAFE, buffer);
                return true;
            }
            // Java 7 or 8
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null) {
                Method clearMethod = cleaner.getClass().getMethod("clean");
                clearMethod.invoke(cleaner);
            }
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    private MemoryUnmapper() {
    }

}
