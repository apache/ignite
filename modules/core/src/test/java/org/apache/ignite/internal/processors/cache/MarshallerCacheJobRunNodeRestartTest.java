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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class MarshallerCacheJobRunNodeRestartTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobRun() throws Exception {
        for (int i = 0; i < 5; i++) {
            U.resolveWorkDirectory(U.defaultWorkDirectory(), DataStorageConfiguration.DFLT_MARSHALLER_PATH, true);

            log.info("Iteration: " + i);

            final int NODES = 3;

            startGridsMultiThreaded(NODES);

            startClientGrid(NODES);

            final IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    for (int i = 0; i < 3; i++) {
                        startGrid(NODES + 1);

                        U.sleep(1000);

                        stopGrid(NODES + 1);
                    }

                    return null;
                }
            });

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer integer) {
                    Ignite ignite = ignite(integer % 4);

                    while (!fut.isDone()) {
                        for (int i = 0; i < 10; i++)
                            ignite.compute().broadcast(job(i));
                    }
                }
            }, (NODES + 1) * 5, "test");

            stopAllGrids();
        }
    }

    /**
     * @param idx Job class index.
     * @return Job.
     */
    static IgniteCallable job(int idx) {
        switch (idx) {
            case 0:
                return new Job1();
            case 1:
                return new Job2();
            case 2:
                return new Job3();
            case 3:
                return new Job4();
            case 4:
                return new Job5();
            case 5:
                return new Job6();
            case 6:
                return new Job7();
            case 7:
                return new Job8();
            case 8:
                return new Job9();
            case 9:
                return new Job10();
            default:
                fail();
        }

        fail();

        return null;
    }

    /**
     *
     */
    static class Job1 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class1();
        }
    }

    /**
     *
     */
    static class Job2 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class2();
        }
    }

    /**
     *
     */
    static class Job3 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class3();
        }
    }

    /**
     *
     */
    static class Job4 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class4();
        }
    }

    /**
     *
     */
    static class Job5 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class5();
        }
    }

    /**
     *
     */
    static class Job6 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class6();
        }
    }

    /**
     *
     */
    static class Job7 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class7();
        }
    }

    /**
     *
     */
    static class Job8 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class8();
        }
    }

    /**
     *
     */
    static class Job9 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class9();
        }
    }

    /**
     *
     */
    static class Job10 implements IgniteCallable {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return new Class10();
        }
    }

    /**
     *
     */
    static class Class1 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class2 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class3 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class4 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class5 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class6 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class7 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class8 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class9 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class Class10 implements Serializable {
        // No-op.
    }
}
