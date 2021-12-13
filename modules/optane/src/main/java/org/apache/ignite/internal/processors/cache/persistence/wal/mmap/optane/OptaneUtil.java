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

package org.apache.ignite.internal.processors.cache.persistence.wal.mmap.optane;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;

/**
 * Contains utils for working with PMDK backed buffers.
 */
public class OptaneUtil {
    /** */
    private static final String extension = ".so";

    /** */
    private OptaneUtil() {
        // No-op.
    }

    /** */
    private static String getLibName() {
        return "/org/apache/ignite/internal/processors/cache/persistence/wal/mmap/optane/"
            + getOSName() + "/" + getOSArch() + "/" + "libmmap" + extension;
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

        try (InputStream in = OptaneUtil.class.getResourceAsStream(libName)) {
            if (in == null) {
                throw new ExceptionInInitializerError("Failed to load native mmap library, "
                    + libName + " not found");
            }

            nativeLib = File.createTempFile("libmmap", extension);

            try (FileOutputStream out = new FileOutputStream(nativeLib)) {
                byte[] buf = new byte[4096];
                int bytesRead;
                while (true) {
                    bytesRead = in.read(buf);
                    if (bytesRead == -1) break;
                    out.write(buf, 0, bytesRead);
                }
            }

            System.load(nativeLib.getAbsolutePath());
        }
        catch (IOException | UnsatisfiedLinkError e) {
            throw new ExceptionInInitializerError(e);
        }
        finally {
            if (nativeLib != null)
                nativeLib.deleteOnExit();
        }
    }

    /**
     * @param segmentIO Instance of {@link SegmentIO}
     * @param szBytes Size of requested buffer.
     */
    public static OptaneByteBufferHolder mmap(SegmentIO segmentIO, int szBytes) {
        Context ctx = mmap(segmentIO.path(), szBytes);

        return new OptaneByteBufferHolder(ctx);
    }

    /**
     * @param path Path to directory to check.
     * @return {@code true} if checked {@code path} are on persistent memory.
     */
    public static boolean isPersistentMemory(String path) {
        return isPmem(path);
    }

    /** */
    static native Context mmap(String path, long size);

    /** */
    static native void munmap(ByteBuffer buffer);

    /** */
    static native void msync(ByteBuffer buffer, long offset, long length, boolean isPmem);

    /** */
    static native boolean isPmem(String path);
}
