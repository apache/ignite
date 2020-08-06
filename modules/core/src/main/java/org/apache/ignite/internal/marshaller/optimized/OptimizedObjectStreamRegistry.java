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

package org.apache.ignite.internal.marshaller.optimized;

import java.io.IOException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutput;

/**
 * Storage for object streams.
 */
abstract class OptimizedObjectStreamRegistry {
    /**
     * Gets output stream.
     *
     * @return Object output stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    abstract OptimizedObjectOutputStream out() throws IgniteInterruptedCheckedException;

    /**
     * Gets input stream.
     *
     * @return Object input stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    abstract OptimizedObjectInputStream in() throws IgniteInterruptedCheckedException;

    /**
     * Closes and releases output stream.
     *
     * @param out Object output stream.
     */
    abstract void closeOut(OptimizedObjectOutputStream out);

    /**
     * Closes and releases input stream.
     *
     * @param in Object input stream.
     */
    abstract void closeIn(OptimizedObjectInputStream in);

    /**
     * Creates output stream.
     *
     * @return Object output stream.
     */
    static OptimizedObjectOutputStream createOut() {
        try {
            return new OptimizedObjectOutputStream(new GridUnsafeDataOutput(4 * 1024));
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create object output stream.", e);
        }
    }

    /**
     * Creates input stream.
     *
     * @return Object input stream.
     */
    static OptimizedObjectInputStream createIn() {
        try {
            return new OptimizedObjectInputStream(new GridUnsafeDataInput());
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create object input stream.", e);
        }
    }
}
