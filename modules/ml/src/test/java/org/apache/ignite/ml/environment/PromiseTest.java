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

package org.apache.ignite.ml.environment;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link Promise} functionality.
 */
public class PromiseTest {
    /** */
    @Test
    public void testUnsafeGet() {
        assertNull("Strategy", new TestPromise().unsafeGet());
    }

    /** */
    @Test
    public void testGetOpt() {
        assertEquals(Optional.empty(), (new TestPromise() {
            /** {@inheritDoc} */
            @Override public Object get() throws ExecutionException {
                throw new ExecutionException("test", new RuntimeException("test cause"));
            }
        }).getOpt());
    }

    /** */
    private static class TestPromise implements Promise<Object> {
        /** {@inheritDoc} */
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public Object get() throws ExecutionException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object get(long timeout, @NotNull TimeUnit unit) {
            return null;
        }
    }
}

