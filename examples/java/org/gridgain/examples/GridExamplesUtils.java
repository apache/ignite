// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridExamplesUtils {
    /**
     * Exits with code {@code -1} if maximum memory is below 90% of minimally allowed threshold.
     *
     * @param min Minimum memory threshold.
     */
    public static void checkMinMemory(long min) {
        long maxMem = Runtime.getRuntime().maxMemory();

        if (maxMem < .85 * min) {
            System.err.println("Heap limit is too low (" + (maxMem / (1024 * 1024)) +
                "MB), please increase heap size at least up to " + (min / (1024 * 1024)) + "MB.");

            System.exit(-1);
        }
    }

    /**
     * @return Resolved GridGain home via system or environment properties.
     * @throws RuntimeException If failed to resolve.
     */
    public static String resolveGridGainHome() throws RuntimeException {
        String var = System.getProperty("GRIDGAIN_HOME");

        if (var == null)
            var = System.getenv("GRIDGAIN_HOME");

        if (var == null)
            throw new RuntimeException("Failed to resolve GridGain home folder " +
                "(please set 'GRIDGAIN_HOME' environment or system variable)");

        return var;
    }

    /**
     * Gets random value from given collection.
     *
     * @param c Input collection (no {@code null} and not emtpy).
     * @param <T> Type of the collection.
     * @return Random value from the input collection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T> T rand(Collection<? extends T> c) {
        A.notNull(c, "c");

        int n = ThreadLocalRandom.current().nextInt(c.size());

        int i = 0;

        for (T t : c) {
            if (i++ == n) {
                return t;
            }
        }

        throw new ConcurrentModificationException();
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * <p>
     * Note that this method follows includes {@link GridMultiException#nestedCauses()}
     * into check.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public static boolean hasCause(@Nullable Throwable t, @Nullable Class<? extends Throwable>... cls) {
        if (t == null || F.isEmpty(cls))
            return false;

        assert cls != null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            for (Class<? extends Throwable> c : cls)
                if (c.isAssignableFrom(th.getClass()))
                    return true;

            if (th instanceof GridMultiException) {
                for (Throwable n : ((GridMultiException)th).nestedCauses())
                    if (hasCause(n, cls))
                        return true;
            }

            if (th.getCause() == th)
                break;
        }

        return false;
    }
}
