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

package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.internal.metastorage.common.OperationType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines operation for meta storage conditional update (invoke).
 */
public final class Operation {
    /** Actual operation implementation. */
    private final InnerOp upd;

    /**
     * Constructs an operation which wraps the actual operation implementation.
     *
     * @param upd The actual operation implementation.
     */
    Operation(InnerOp upd) {
        this.upd = upd;
    }

    /** */
    public InnerOp inner() {
        return upd;
    }

    /**
     * Returns operation type.
     *
     * @return Operation type.
     */
    public OperationType type() {
        return upd.type();
    }

    /**
     * Represents operation of type <i>remove</i>.
     */
    public static final class RemoveOp extends AbstractOp {
        /**
         * Default no-op constructor.
         *
         * @param key Identifies an entry which operation will be applied to.
         */
        RemoveOp(byte[] key) {
            super(key, OperationType.REMOVE);
        }
    }

    /**
     * Represents operation of type <i>put</i>.
     */
    public static final class PutOp extends AbstractOp {
        /** Value. */
        private final byte[] val;

        /**
         * Constructs operation of type <i>put</i>.
         *
         * @param key Identifies an entry which operation will be applied to.
         * @param val The value to which the entry should be updated.
         */
        PutOp(byte[] key, byte[] val) {
            super(key, OperationType.PUT);

            this.val = val;
        }

        /**
         * Returns value.
         *
         * @return Value.
         */
        public byte[] value() {
            return val;
        }
    }

    /**
     * Represents operation of type <i>no-op</i>.
     */
    public static final class NoOp extends AbstractOp {
        /**
         * Default no-op constructor.
         */
        NoOp() {
            super(null, OperationType.NO_OP);
        }
    }

    /**
     * Defines operation interface.
     */
    public interface InnerOp {
        /**
         * Returns key.
         *
         * @return Key.
         */
        @Nullable byte[] key();

        /**
         * Returns operation type.
         *
         * @return Operation type.
         */
        @NotNull OperationType type();
    }

    /** */
    private static class AbstractOp implements InnerOp {
        /** Key. */
        @Nullable private final byte[] key;

        /** Operation type. */
        @NotNull private final OperationType type;

        /**
         * Ctor.
         * @param key Key.
         * @param type Operation type.
         */
        private AbstractOp(@Nullable byte[] key, OperationType type) {
            this.key = key;
            this.type = type;
        }

        /**
         * Returns key.
         *
         * @return Key.
         */
        @Nullable
        @Override public byte[] key() {
            return key;
        }

        /**
         * Returns operation type.
         *
         * @return Operation type.
         */
        @NotNull
        @Override public OperationType type() {
            return type;
        }
    }
}
