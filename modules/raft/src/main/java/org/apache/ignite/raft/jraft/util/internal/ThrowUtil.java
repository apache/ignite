/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.internal;

import org.jetbrains.annotations.Nullable;

/**
 * Throwing tool.
 */
public final class ThrowUtil {
    /**
     * Raises an exception bypassing compiler checks for checked exceptions.
     */
    public static void throwException(final Throwable t) {
        ThrowUtil.throwException0(t);
    }

    /**
     * private static <E extends java/lang/Throwable> void throwException0(java.lang.Throwable) throws E; flags:
     * ACC_PRIVATE, ACC_STATIC Code: stack=1, locals=1, args_size=1 0: aload_0 1: athrow ... Exceptions: throws
     * java.lang.Throwable
     */
    private static <E extends Throwable> void throwException0(final Throwable t) throws E {
        throw (E) t;
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param msg Message text that should be in cause.
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public static boolean hasCause(@Nullable Throwable t, @Nullable String msg, Class<?> @Nullable... cls) {
        if (t == null || cls == null || cls.length == 0)
            return false;

        for (Throwable th = t; th != null; th = th.getCause()) {
            for (Class<?> c : cls) {
                if (c.isAssignableFrom(th.getClass())) {
                    if (msg != null) {
                        if (th.getMessage() != null && th.getMessage().contains(msg))
                            return true;
                        else
                            continue;
                    }

                    return true;
                }
            }

            for (Throwable n : th.getSuppressed()) {
                if (hasCause(n, msg, cls))
                    return true;
            }

            if (th.getCause() == th)
                break;
        }

        return false;
    }

    private ThrowUtil() {
    }
}
