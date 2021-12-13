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

/** */
public class NumaAllocUtil {
    /** */
    private static final String extension = ".so";

    /** */
    private NumaAllocUtil() {
        // No-op.
    }

    /** */
    private static String getLibName() {
        return "/org/apache/ignite/internal/mem/"
            + getOSName() + "/" + getOSArch() + "/" + "libnuma_alloc" + extension;
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

            nativeLib = File.createTempFile("libnuma_alloc", extension);

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

    /** */
    public static native long allocate(long size, int node);

    /** */
    public static native long allocateLocal(long size);

    /** */
    public static native long allocateInterleaved(long size, int[] nodes);

    /** */
    public static native void free(long addr);
}
