/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.util;

import org.gridgain.client.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * Java client utils.
 */
public abstract class GridClientUtils {
    /**
     * Closes resource without reporting any error.
     *
     * @param closeable Resource to close.
     */
    public static void closeQuiet(@Nullable Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Creates a predicates that checks if given value is contained in collection c.
     *
     * @param c Collection to check.
     * @param <T> Type of elements in collection.
     * @return Predicate.
     */
    public static <T> GridClientPredicate<T> contains(final Collection<T> c) {
        return new GridClientPredicate<T>() {
            @Override public boolean apply(T t) {
                return (!(c == null || c.isEmpty())) && c.contains(t);
            }
        };
    }

    /**
     * Gets first element from given collection or returns {@code null} if the collection is empty.
     *
     * @param c A collection.
     * @param <T> Type of the collection.
     * @return Collections' first element or {@code null} in case if the collection is empty.
     */
    @Nullable public static <T> T first(@Nullable Iterable<? extends T> c) {
        if (c == null)
            return null;

        Iterator<? extends T> it = c.iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Applies filter and returns filtered collection of nodes.
     *
     * @param elements Nodes to be filtered.
     * @param filters Filters to apply. Elements of this array are allowed to be {@code null}.
     * @return Filtered collection.
     */
    public static <T> Collection<T> applyFilter(Iterable<? extends T> elements,
        GridClientPredicate<? super T>... filters) {
        assert filters != null;

        Collection<T> res = new LinkedList<>();

        for (T e : elements) {
            boolean add = true;

            for (GridClientPredicate<? super T> filter : filters)
                if (filter != null && !filter.apply(e)) {
                    add = false;

                    break;
                }

            if (add)
                res.add(e);
        }

        return res;
    }

    /**
     * Checks if given REST protocol available for given node.
     *
     * @param node Node.
     * @param p Protocol.
     * @return {@code true} if protocol {@code p} available for {@code node},
     *  {@code false} otherwise.
     */
    public static boolean restAvailable(GridClientNode node, GridClientProtocol p) {
        return !node.availableAddresses(p, false).isEmpty();
    }

    /**
     * Shutdowns given {@code ExecutorService} and wait for executor service to stop.
     *
     * @param owner The ExecutorService owner.
     * @param exec ExecutorService to shutdown.
     * @param log The logger to possible exceptions and warnings.
     */
    public static void shutdownNow(Class<?> owner, ExecutorService exec, Logger log) {
        if (exec != null) {
            List<Runnable> tasks = exec.shutdownNow();

            if (!tasks.isEmpty())
                log.warning("Runnable tasks outlived thread pool executor service [owner=" + getSimpleName(owner) +
                    ", tasks=" + tasks + ']');

            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                log.warning("Got interrupted while waiting for executor service to stop.");

                exec.shutdownNow();

                // Preserve interrupt status.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Gets simple class name taking care of empty names.
     *
     * @param cls Class to get the name for.
     * @return Simple class name.
     */
    public static String getSimpleName(Class<?> cls) {
        return cls.getSimpleName().isEmpty() ? cls.getName() : cls.getSimpleName();
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }
}
