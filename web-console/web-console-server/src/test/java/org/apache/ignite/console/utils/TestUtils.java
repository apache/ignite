

package org.apache.ignite.console.utils;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Various test utils.
 */
public class TestUtils {
    /**
     * Stop all grids.
     */
    public static void stopAllGrids() {
        Ignition.stopAll(true);
    }

    /**
     * Cleanup persistence folder.
     *
     * @throws IgniteCheckedException If failed.
     */
    public static void cleanPersistenceDir() throws IgniteCheckedException {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }
}
