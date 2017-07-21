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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.lang.IgniteCallable;

/**
 * AtomicReference and AtomicStamped multi node tests.
 */
public abstract class GridCacheAtomicReferenceMultiNodeAbstractTest extends IgniteAtomicsAbstractTest {
    /** */
    protected static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAtomicReference() throws Exception {
        // Get random name of reference.
        final String refName = UUID.randomUUID().toString();
        // Get random value of atomic reference.
        final String val = UUID.randomUUID().toString();
        // Get random new value of atomic reference.
        final String newVal = UUID.randomUUID().toString();

        // Initialize atomicReference in cache.
        IgniteAtomicReference<String> ref = grid(0).atomicReference(refName, val, true);

        final Ignite ignite = grid(0);

        // Execute task on all grid nodes.
        ignite.compute().call(new IgniteCallable<Object>() {
            @Override public String call() {
                IgniteAtomicReference<String> ref = ignite.atomicReference(refName, val, true);

                assertEquals(val, ref.get());

                return ref.get();
            }
        });

        ref.compareAndSet("WRONG EXPECTED VALUE", newVal);

        // Execute task on all grid nodes.
        ignite.compute().call(new IgniteCallable<String>() {
            @Override public String call() {
                IgniteAtomicReference<String> ref = ignite.atomicReference(refName, val, true);

                assertEquals(val, ref.get());

                return ref.get();
            }
        });

        ref.compareAndSet(val, newVal);

        // Execute task on all grid nodes.
        ignite.compute().call(new IgniteCallable<String>() {
            @Override public String call() {
                IgniteAtomicReference<String> ref = ignite.atomicReference(refName, val, true);

                assertEquals(newVal, ref.get());

                return ref.get();
            }
        });
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAtomicStamped() throws Exception {
        // Get random name of stamped.
        final String stampedName = UUID.randomUUID().toString();
        // Get random value of atomic stamped.
        final String val = UUID.randomUUID().toString();
        // Get random value of atomic stamped.
        final String stamp = UUID.randomUUID().toString();
        // Get random new value of atomic stamped.
        final String newVal = UUID.randomUUID().toString();
        // Get random new stamp of atomic stamped.
        final String newStamp = UUID.randomUUID().toString();

        // Initialize atomicStamped in cache.
        IgniteAtomicStamped<String, String> stamped = grid(0).atomicStamped(stampedName, val, stamp, true);

        final Ignite ignite = grid(0);

        // Execute task on all grid nodes.
        ignite.compute().call(new IgniteCallable<String>() {
            @Override public String call() {
                IgniteAtomicStamped<String, String> stamped = ignite.atomicStamped(stampedName, val, stamp, true);

                assertEquals(val, stamped.value());
                assertEquals(stamp, stamped.stamp());

                return stamped.value();
            }
        });

        stamped.compareAndSet("WRONG EXPECTED VALUE", newVal, "WRONG EXPECTED STAMP", newStamp);

        // Execute task on all grid nodes.
        ignite.compute().call(new IgniteCallable<String>() {
            @Override public String call() {
                IgniteAtomicStamped<String, String> stamped = ignite.atomicStamped(stampedName, val, stamp, true);

                assertEquals(val, stamped.value());
                assertEquals(stamp, stamped.stamp());

                return stamped.value();
            }
        });

        stamped.compareAndSet(val, newVal, stamp, newStamp);

        // Execute task on all grid nodes.
        ignite.compute().call(new IgniteCallable<String>() {
            @Override public String call() {
                IgniteAtomicStamped<String, String> stamped = ignite.atomicStamped(stampedName, val, stamp, true);

                assertEquals(newVal, stamped.value());
                assertEquals(newStamp, stamped.stamp());

                return stamped.value();
            }
        });
    }
}