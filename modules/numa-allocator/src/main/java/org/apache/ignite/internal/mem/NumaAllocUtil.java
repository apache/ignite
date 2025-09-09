/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.mem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

/** */
public class NumaAllocUtil {
    /** */
    private static final String extension = ".so";

    /** */
    private static final String libraryName = "libnuma_alloc";

    /** */
    private NumaAllocUtil() {
        // No-op.
    }

    /** */
    private static String getLibName() {
        return Paths.get("/org/apache/ignite/internal/mem", getOSName(), getOSArch(), libraryName + extension)
            .toString();
    }

    /** */
    private static String getOSArch() {
        return System.getProperty("os.arch", "");
    }

    /** */
    private static String getOSName() {
        if (System.getProperty("os.name", "").contains("Linux"))
            return "linux";
        else
            throw new UnsupportedOperationException("Operating System is not supported");
    }

    static {
        String libName = getLibName();
        File nativeLib = null;

        try (InputStream in = NumaAllocUtil.class.getResourceAsStream(libName)) {
            if (in == null) {
                throw new ExceptionInInitializerError("Failed to load native numa_alloc library, "
                    + libName + " not found");
            }

            nativeLib = File.createTempFile(libraryName, extension);

            try (FileOutputStream out = new FileOutputStream(nativeLib)) {
                byte[] buf = new byte[4096];

                int bytesRead;
                while ((bytesRead = in.read(buf)) > 0)
                    out.write(buf, 0, bytesRead);
            }

            System.load(nativeLib.getAbsolutePath());

            NUMA_NODES_CNT = nodesCount();
        }
        catch (IOException | UnsatisfiedLinkError e) {
            throw new ExceptionInInitializerError(e);
        }
        finally {
            if (nativeLib != null)
                nativeLib.deleteOnExit();
        }
    }

    /** */
    public static final int NUMA_NODES_CNT;

    /**
     * Allocate memory using using default NUMA memory policy of current thread.
     * Uses {@code void *numa_alloc(size_t)} under the hood.
     * <p>
     * @param size Size of buffer.
     * @return Address of buffer.
     */
    public static native long allocate(long size);

    /**
     * Allocate memory on specific NUMA node. Uses {@code void *numa_alloc_onnode(size_t)} under the hood.
     * <p>
     * @param size Size of buffer.
     * @param node NUMA node.
     * @return Address of buffer.
     */
    public static native long allocateOnNode(long size, int node);

    /**
     * Allocate memory on local NUMA node. Uses {@code void *numa_alloc_local(size_t)} under the hood.
     * <p>
     * @param size Size of buffer.
     * @return Address of buffer.
     */
    public static native long allocateLocal(long size);

    /**
     * Allocate memory interleaved on NUMA nodes. Uses
     * {@code void *numa_alloc_interleaved_subset(size_t, struct bitmask*)} under the hood.
     * <p>
     * @param size Size of buffer.
     * @param nodes Array of nodes allocate on. If {@code null} or empty, allocate on all NODES.
     * @return Address of buffer.
     */
    public static native long allocateInterleaved(long size, int[] nodes);

    /**
     * Get allocated buffer size.
     *
     * @param addr Address of buffer.
     * @return Size of buffer.
     */
    public static native long chunkSize(long addr);

    /**
     * Free allocated memory.
     *
     * @param addr Address of buffer.
     */
    public static native void free(long addr);

    /**
     * Get NUMA nodes count.
     *
     * @return NUMA nodes count available on system.
     */
    private static native int nodesCount();
}
