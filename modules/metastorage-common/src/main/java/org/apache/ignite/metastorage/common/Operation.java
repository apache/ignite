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

package org.apache.ignite.metastorage.common;

/**
 * Defines operation for meta storage conditional update (invoke).
 */
public final class Operation {
    /** Actual operation implementation. */
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final InnerOp upd;

    /**
     * Constructs an operation which wraps the actual operation implementation.
     *
     * @param upd The actual operation implementation.
     */
    Operation(InnerOp upd) {
        this.upd = upd;
    }

    /**
     * Represents operation of type <i>remove</i>.
     */
    public static final class RemoveOp implements InnerOp {
        /** Key. */
        private final Key key;

        /**
         * Creates a new remove operation for the given {@code key}.
         *
         * @param key Key.
         */
        RemoveOp(Key key) {
            this.key = key;
        }
    }

    /**
     * Represents operation of type <i>put</i>.
     */
    public static final class PutOp implements InnerOp {
        /** Key. */
        private final Key key;

        /** Value. */
        private final byte[] val;

        /**
         * Constructs operation of type <i>put</i>.
         *
         * @param val The value to which the entry should be updated.
         */
        PutOp(Key key, byte[] val) {
            this.key = key;
            this.val = val;
        }
    }

    /**
     * Represents operation of type <i>no-op</i>.
     */
    public static final class NoOp implements InnerOp {
        /**
         * Default no-op constructor.
         */
        NoOp() {
            // No-op.
        }
    }

    /**
     * Defines operation interface.
     */
    private interface InnerOp {
        // Marker interface.
    }
}
