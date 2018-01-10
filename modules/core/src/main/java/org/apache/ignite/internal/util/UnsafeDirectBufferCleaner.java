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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;

/**
 * {@link DirectBufferCleaner} implementation based on {@code Unsafe.invokeCleaner} method.
 *
 * Note: This implementation will work only for Java 9+.
 */
public class UnsafeDirectBufferCleaner implements DirectBufferCleaner {
    /** Cleaner method. */
    private final Method cleanerMtd;

    /** */
    public UnsafeDirectBufferCleaner() {
        try {
            cleanerMtd = Unsafe.class.getMethod("invokeCleaner", ByteBuffer.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Reflection failure: no sun.misc.Unsafe.invokeCleaner() method found", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clean(ByteBuffer buf) {
        GridUnsafe.invoke(cleanerMtd, buf);
    }
}
