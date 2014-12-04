/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.apache.ignite.compute.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;
import java.util.*;
import java.util.jar.*;

/**
 * Helper that loads classes either from directory or from JAR file.
 * <p>
 * If loading from directory is used, helper scans given directory
 * and all subdirectories recursively and loads all files
 * with ".class" extension by given class loader. If class could not
 * be loaded it will be ignored.
 * <p>
 * If JAR file loading is used helper scans JAR file and tries to
 * load all {@link JarEntry} assuming it's a file name.
 * If at least one of them could not be loaded helper fails.
 */
final class GridUriDeploymentDiscovery {
    /**
     * Enforces singleton.
     */
    private GridUriDeploymentDiscovery() {
        // No-op.
    }

    /**
     * Load classes from given file. File could be either directory or JAR file.
     *
     * @param clsLdr Class loader to load files.
     * @param file Either directory or JAR file which contains classes or
     *      references to them.
     * @return Set of found and loaded classes or empty set if file does not
     *      exist.
     * @throws GridSpiException Thrown if given JAR file references to none
     *      existed class or IOException occurred during processing.
     */
    static Set<Class<? extends GridComputeTask<?, ?>>> getClasses(ClassLoader clsLdr, File file)
        throws GridSpiException {
        Set<Class<? extends GridComputeTask<?, ?>>> rsrcs = new HashSet<>();

        if (file.exists() == false)
            return rsrcs;

        GridUriDeploymentFileResourceLoader fileRsrcLdr = new GridUriDeploymentFileResourceLoader(clsLdr, file);

        if (file.isDirectory())
            findResourcesInDirectory(fileRsrcLdr, file, rsrcs);
        else {
            try {
                for (JarEntry entry : U.asIterable(new JarFile(file.getAbsolutePath()).entries())) {
                    Class<? extends GridComputeTask<?, ?>> rsrc = fileRsrcLdr.createResource(entry.getName(), false);

                    if (rsrc != null)
                        rsrcs.add(rsrc);
                }
            }
            catch (IOException e) {
                throw new GridSpiException("Failed to discover classes in file: " + file.getAbsolutePath(), e);
            }
        }

        return rsrcs;
    }

    /**
     * Recursively scans given directory and load all found files by loader.
     *
     * @param clsLdr Loader that could load class from given file.
     * @param dir Directory which should be scanned.
     * @param rsrcs Set which will be filled in.
     */
    @SuppressWarnings({"UnusedCatchParameter"})
    private static void findResourcesInDirectory(GridUriDeploymentFileResourceLoader clsLdr, File dir,
        Set<Class<? extends GridComputeTask<?, ?>>> rsrcs) {
        assert dir.isDirectory() == true;

        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                // Recurse down into directories.
                findResourcesInDirectory(clsLdr, file, rsrcs);
            }
            else {
                Class<? extends GridComputeTask<?, ?>> rsrc = null;

                try {
                    rsrc = clsLdr.createResource(file.getAbsolutePath(), true);
                }
                catch (GridSpiException e) {
                    // Must never happen because we use 'ignoreUnknownRsrc=true'.
                    assert false;
                }

                if (rsrc != null)
                    rsrcs.add(rsrc);
            }
        }
    }
}
