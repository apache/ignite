/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.jdk8.backport;

import sun.misc.Unsafe;

import java.lang.reflect.*;
import java.security.*;

/**
 * Provides handle on Unsafe class from SUN which cannot be instantiated directly.
 */
public class GridUnsafe {
    /**
     * Ensure singleton.
     */
    private GridUnsafe() {
        // No-op.
    }

    /**
     * @return Instance of Unsafe class.
     */
    public static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored) {
            try {
                return AccessController.doPrivileged
                    (new PrivilegedExceptionAction<Unsafe>() {
                        @Override public Unsafe run() throws Exception {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");

                            f.setAccessible(true);

                            return (Unsafe)f.get(null);
                        }
                    });
            }
            catch (PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics.", e.getCause());
            }
        }
    }
}
