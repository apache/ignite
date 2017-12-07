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

public class IgniteNativeIoLib {

    static {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    public static native int open(String pathname, int flags, int mode);


    /**
     * See "man 2 close"
     *
     * @param fd The file descriptor of the file to close
     *
     * @return 0 on success, -1 on error
     */
    public static native int close(int fd);

    public static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);

    public static native NativeLong write(int fd,  Pointer buf, NativeLong count);

    public static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

    public static native int fsync(int fd);

    public static native int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size);

    public static native void free(Pointer ptr);
    public static native String strerror(int errnum);

}
