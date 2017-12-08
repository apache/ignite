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

package org.apache.ignite.internal.processors.cache.persistence.file;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

@SuppressWarnings("OctalInteger")
public class IgniteNativeIoLib {

    private static boolean jnaAvailable;

    private static Exception ex = null;

    static {
        if (Platform.isLinux()) {
            try {
                if (checkLinuxVersion()) {
                    Native.register(Platform.C_LIBRARY_NAME);
                    jnaAvailable = true;
                }
                else
                    jnaAvailable = false;
            }
            catch (Exception e) {
                e.printStackTrace();
                ex = e;
                jnaAvailable = false;
            }
        }
        else {
            jnaAvailable = false;
        }
    }

    /**
     *
     * O_DIRECT  support was added under Linux in kernel version 2.4.10.
     * @return {@code true} if O_DIRECT is supported, kernel version >= 2.4.10
     */
    private static boolean checkLinuxVersion() {

        String osVer = System.getProperty("os.version");

        if (osVer == null)
            return false;

        List<Integer> verComps = new ArrayList<>();

        for (StringTokenizer tokenizer = new StringTokenizer(osVer, ".-"); tokenizer.hasMoreTokens(); ) {
            String verComp = tokenizer.nextToken();
            if (verComp.matches("\\d*")) {
                verComps.add(Integer.parseInt(verComp));
            }

        }

        if (verComps.isEmpty())
            return false;

        final int versionIdx = 0;
        final int majorRevIdx = 1;
        final int minorRevIdx = 2;

        if (verComps.get(versionIdx) > 2) {
            return true;
        }
        else if (verComps.get(versionIdx) == 2) {
            int compsCnt = verComps.size();
            if (compsCnt > majorRevIdx && verComps.get(majorRevIdx) > 4) {
                return true;
            }
            else if (compsCnt > minorRevIdx
                && verComps.get(majorRevIdx) == 4
                && verComps.get(minorRevIdx) >= 10) {
                return true;
            }
        }
        return false;
    }

    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04000000;

    /**
     * Calculate Lowest Common Multiplier
     */
    private static long LCM(long a, long b) {
        return (a * b) / GCF(a, b);
    }

    /**
     * Calculate Greatest Common Factor
     */
    private static long GCF(long a, long b) {
        if (b == 0) {
            return a;
        }
        else {
            return (GCF(b, a % b));
        }
    }

    public static int getFsBlockSize(String storageDir, IgniteLogger log) {
        int fsBlockSize = -1;

        if (jnaAvailable) {
            // get file system block size for use with workingDir
            // see "man 3 posix_memalign" for why we do this
            final int _PC_REC_XFER_ALIGN = 0x11;

            int pcAlign = pathconf(storageDir, _PC_REC_XFER_ALIGN).intValue();
            if (log.isInfoEnabled()) {
                log.info("Page size configuration for storage path [" + storageDir + "]: " + pcAlign);
            }
            if (pcAlign > 0)
                fsBlockSize = pcAlign;

            int pageSize = getpagesize();

            if (log.isInfoEnabled()) {
                log.info("Linux memory page size: " + pageSize);
            }

            fsBlockSize = (int)LCM(fsBlockSize, pageSize);

            // just being completely paranoid:
            // (512 is the rule for 2.6+ kernels as mentioned before)
            fsBlockSize = (int)LCM(fsBlockSize, 512);

            // lastly, a sanity check
            if (fsBlockSize <= 0 || ((fsBlockSize & (fsBlockSize - 1)) != 0)) {
                U.warn(log, "file system block size should be a power of two, was found to be " + fsBlockSize);
                U.warn(log, "Disabling O_DIRECT support");
                return -1;
            }
            if (log.isInfoEnabled()) {
                log.info("Selected FS block size : " + fsBlockSize);
            }
        }
        else {
            log.info("JNA support ");
            if (ex != null) {
                U.warn(log, "Failed to initialize O_DIRECT support", ex);
            }
        }

        return fsBlockSize;
    }

    public static native int open(String pathname, int flags, int mode);

    /**
     * See "man 2 close"
     *
     * @param fd The file descriptor of the file to close
     * @return 0 on success, -1 on error
     */
    public static native int close(int fd);

    public static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);

    public static native NativeLong write(int fd, Pointer buf, NativeLong count);

    public static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

    public static native NativeLong read(int fd, Pointer buf, NativeLong count);

    public static native int fsync(int fd);

    public static native int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size);

    public static native void free(Pointer ptr);

    public static native String strerror(int errnum);

    /**
     * On many systems there are alignment restrictions, for example, on buffers used for direct block device I/O.
     * POSIX specifies the pathconf(path,_PC_REC_XFER_ALIGN) call that tells what alignment is needed.
     *
     * @param path
     */
    public static native NativeLong pathconf(String path, int name);

    /**
     * The function getpagesize() returns the number of bytes in a memory
     * page, where "page" is a fixed-length block, the unit for memory
     * allocation and file mapping
     */
    public static native int getpagesize();
}
