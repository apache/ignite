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

package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import java.io.DataInput;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Multimap for hadoop intermediate results.
 */
@SuppressWarnings("PublicInnerClass")
public interface HadoopMultimap extends AutoCloseable {
    /**
     * Incrementally visits all the keys and values in the map.
     *
     * @param ignoreLastVisited Flag indicating that visiting must be started from the beginning.
     * @param v Visitor.
     * @return {@code false} If visiting was impossible.
     */
    public boolean visit(boolean ignoreLastVisited, Visitor v) throws IgniteCheckedException;

    /**
     * @param ctx Task context.
     * @return Adder.
     * @throws IgniteCheckedException If failed.
     */
    public Adder startAdding(HadoopTaskContext ctx) throws IgniteCheckedException;

    /**
     * @param taskCtx Task context.
     * @return Task input.
     * @throws IgniteCheckedException If failed.
     */
    public HadoopTaskInput input(HadoopTaskContext taskCtx)
        throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public void close();

    /**
     * Adder.
     */
    public interface Adder extends HadoopTaskOutput {
        /**
         * @param in Data input.
         * @param reuse Reusable key.
         * @return Key.
         * @throws IgniteCheckedException If failed.
         */
        public Key addKey(DataInput in, @Nullable Key reuse) throws IgniteCheckedException;
    }

    /**
     * Key add values to.
     */
    public interface Key {
        /**
         * @param val Value.
         */
        public void add(Value val);
    }

    /**
     * Value.
     */
    public interface Value {
        /**
         * @return Size in bytes.
         */
        public int size();

        /**
         * @param ptr Pointer.
         */
        public void copyTo(long ptr);
    }

    /**
     * Key and values visitor.
     */
    public interface Visitor {
        /**
         * @param keyPtr Key pointer.
         * @param keySize Key size.
         */
        public void onKey(long keyPtr, int keySize) throws IgniteCheckedException;

        /**
         * @param valPtr Value pointer.
         * @param valSize Value size.
         */
        public void onValue(long valPtr, int valSize) throws IgniteCheckedException;
    }
}