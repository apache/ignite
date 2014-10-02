/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.license;

import org.gridgain.grid.util.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Usage registry.
 */
public class GridLicenseUseRegistry {
    /** Usage map. */
    private static final ConcurrentMap<GridLicenseSubsystem, Collection<Class<?>>> useMap =
        new ConcurrentHashMap8<>();

    /**
     * Ensure singleton.
     */
    private GridLicenseUseRegistry() {
        // No-op.
    }

    /**
     * Callback for whenever component gets used.
     *
     * @param ed Subsystem.
     * @param cls Component.
     */
    public static void onUsage(GridLicenseSubsystem ed, Class<?> cls) {
        Collection<Class<?>> c = useMap.get(ed);

        if (c == null) {
            Collection<Class<?>> old = useMap.putIfAbsent(ed, c = new GridConcurrentHashSet<>());

            if (old != null)
                c = old;
        }

        c.add(cls);
    }

    /**
     * Gets used subsystems for given subsystem.
     *
     * @param ed Subsystem.
     * @return Component.
     */
    public static Collection<Class<?>> usedClasses(GridLicenseSubsystem ed) {
        Collection<Class<?>> c = useMap.get(ed);

        return c == null ? Collections.<Class<?>>emptySet() : c;
    }

    /**
     * Checks if subsystem is used.
     *
     * @param ed Subsystem to check.
     * @return {@code True} if used.
     */
    public static boolean used(GridLicenseSubsystem ed) {
        return !usedClasses(ed).isEmpty();
    }

    /**
     * Clears usages.
     *
     * FOR TESTING PURPOSES ONLY!
     */
    public static void clear() {
        useMap.clear();
    }
}
