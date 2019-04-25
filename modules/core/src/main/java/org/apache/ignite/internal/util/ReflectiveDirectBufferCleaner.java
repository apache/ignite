/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * {@link DirectBufferCleaner} implementation based on {@code sun.misc.Cleaner} and
 * {@code sun.nio.ch.DirectBuffer.cleaner()} method.
 *
 * Mote: This implementation will not work on Java 9+.
 */
public class ReflectiveDirectBufferCleaner implements DirectBufferCleaner {
    /** Cleaner method. */
    private final Method cleanerMtd;

    /** Clean method. */
    private final Method cleanMtd;

    /** */
    public ReflectiveDirectBufferCleaner() {
        try {
            cleanerMtd = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");

        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("No sun.nio.ch.DirectBuffer.cleaner() method found", e);
        }

        try {
            cleanMtd = Class.forName("sun.misc.Cleaner").getMethod("clean");
        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("No sun.misc.Cleaner.clean() method found", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clean(ByteBuffer buf) {
        try {
            cleanMtd.invoke(cleanerMtd.invoke(buf));
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to invoke direct buffer cleaner", e);
        }
    }
}
