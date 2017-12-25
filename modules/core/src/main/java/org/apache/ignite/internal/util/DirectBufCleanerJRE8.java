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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Java8-specific implementation of {@link DirectBufCleaner}
 */
public class DirectBufCleanerJRE8 implements DirectBufCleaner {
    /** Cleaner method. */
    private final Method cleanerMtd;

    /** Clean method. */
    private final Method cleanMtd;

    /** */
    public DirectBufCleanerJRE8() {
        try {
            cleanerMtd = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");

        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("Reflection failure: no sun.nio.ch.DirectBuffer.cleaner() method found", e);
        }

        try {
            cleanMtd = Class.forName("sun.misc.Cleaner").getMethod("clean");
        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("Reflection failure: no sun.misc.Cleaner.clean() method found", e);
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
