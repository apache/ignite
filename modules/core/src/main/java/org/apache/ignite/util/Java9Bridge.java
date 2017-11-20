package org.apache.ignite.util;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.IgniteUtils;


import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipFile;


/**
 * Utility method for Java 8/9 interop
 */
public class Java9Bridge {

    private final static boolean JAVA_9 = System.getProperty("java.version").startsWith("9");


    /**
     * Returns urls for the classloader
     *
     * @param ldr classloader in which to find urls
     * @return list of urls
     */
    public static List<URL> getUrls(ClassLoader ldr) {

        try {
            if (!JAVA_9) {
                URLClassLoader cl = (URLClassLoader) ldr;
                return new ArrayList<>(Arrays.asList(cl.getURLs()));

            } else {
                Field ucpField = Class.forName("jdk.internal.loader.BuiltinClassLoader")
                        .getDeclaredField("ucp");
                ucpField.setAccessible(true);

                Object ucpObject = ucpField.get(ldr);
                URL[] urls = (URL[]) Class.forName("jdk.internal.loader.URLClassPath")
                        .getMethod("getURLs").invoke(ucpObject);

                return new ArrayList<>(Arrays.asList(urls));

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    /**
     * Closes ClassLoader and logs
     * @param clsLdr
     * @param log
     */

    public static void close(ClassLoader clsLdr, IgniteLogger log) {
        if (clsLdr == null)
            return;

        Object loaders;

        try {

            Field ucpField = clsLdr.getClass().getDeclaredField("ucp");
            ucpField.setAccessible(true);

            if (!JAVA_9) {

                Object ucpObject=ucpField.get(clsLdr);
                Field ldrFld = Class.forName("sun.misc.URLClassPath")
                        .getDeclaredField("loaders");
                ldrFld.setAccessible(true);
                loaders = ldrFld.get(ucpObject);
            } else {


                Field ldrFld = Class.forName("jdk.internal.loader.URLClassPath")
                        .getField("loaders");
                ldrFld.setAccessible(true);
                loaders = ucpField.get(clsLdr);
            }

            Iterable ldrs = (Iterable) loaders;

            for (Object ldr : ldrs)
                if (ldr.getClass().getName().endsWith("JarLoader"))
                    try {
                        Field jarFld = ldr.getClass().getDeclaredField("jar");

                        jarFld.setAccessible(true);

                        ZipFile jar = (ZipFile) jarFld.get(ldr);

                        jar.close();
                    } catch (Exception e) {
                        IgniteUtils.warn(log, "Failed to close resource: " + e.getMessage());
                    }

        } catch (Exception e) {
            IgniteUtils.warn(log, "Failed to close resource: " + e.getMessage());
        }


    }
}
