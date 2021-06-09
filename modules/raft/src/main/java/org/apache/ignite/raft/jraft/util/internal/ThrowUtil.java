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

/**
 * Throwing tool.
 */
public final class ThrowUtil {

    private static final ReferenceFieldUpdater<Throwable, Throwable> causeUpdater = Updaters.newReferenceFieldUpdater(
        Throwable.class, "cause");

    /**
     * Raises an exception bypassing compiler checks for checked exceptions.
     */
    public static void throwException(final Throwable t) {
//        if (UnsafeUtil.hasUnsafe()) {
//            UnsafeUtil.throwException(t);
//        } else {
//            ThrowUtil.throwException0(t);
//        }

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

    public static <T extends Throwable> T cutCause(final T cause) {
        Throwable rootCause = cause;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }

        if (rootCause != cause) {
            cause.setStackTrace(rootCause.getStackTrace());
            causeUpdater.set(cause, cause);
        }
        return cause;
    }

    private ThrowUtil() {
    }
}
