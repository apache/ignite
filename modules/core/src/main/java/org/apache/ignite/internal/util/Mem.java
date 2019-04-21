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

package org.apache.ignite.internal.util;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.ptr.PointerByReference;

/**
 *
 */
public class Mem {
    static {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    /** */
    public static native int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size);

    public static final int POSIX_MADV_NORMAL     = 0;
    public static final int POSIX_MADV_RANDOM     = 1;
    public static final int POSIX_MADV_SEQUENTIAL = 2;
    public static final int POSIX_MADV_WILLNEED   = 3;
    public static final int POSIX_MADV_DONTNEED   = 4;

    /** */
    public static native int posix_madvise(long addr, long len, int advice);

    /** */
    public static native int getpagesize();
}
