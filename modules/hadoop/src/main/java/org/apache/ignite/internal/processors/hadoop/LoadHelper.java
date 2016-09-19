package org.apache.ignite.internal.processors.hadoop;

import java.lang.reflect.Method;

/**
 * Should be loaded with the
 */
public class LoadHelper {

    private static Method method;

    static {
        try {
            method = ClassLoader.class.getDeclaredMethod("loadLibrary",
                new Class[] {Class.class, String.class, boolean.class});

            method.setAccessible(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Utility method that loads given class by name with the given "caller" class.
     *
     * @return 'true' on success.
     */
    public static boolean tryLoad(Class caller, String libName) {
        try {
            method.invoke(null, new Object[] {caller/*caller class*/, libName /*lib*/ , false/*isAbsolute*/ });

            return true;
        }
        catch (Throwable t) {
            // TODO:
            t.printStackTrace();

            return false;
        }
    }

}
