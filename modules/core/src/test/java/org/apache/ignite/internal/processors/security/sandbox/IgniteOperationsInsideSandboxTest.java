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

package org.apache.ignite.internal.processors.security.sandbox;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * A user-defined code inside the sandbox can use the public API of Ignite without additional
 * sandbox permissions.
 */
public class IgniteOperationsInsideSandboxTest extends AbstractSandboxTest {
    /** Test compute task. */
    private static final ComputeTask<Object, Object> TEST_COMPUTE_TASK = new ComputeTask<Object, Object>() {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // No-op.
                    }

                    @Override public Object execute() {
                        return null;
                    }
                }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
            );
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    };

    /** Test callable. */
    private static final IgniteCallable<Object> TEST_CALLABLE = new IgniteCallable<Object>() {
        @Override public Object call() {
            return null;
        }
    };

    /** Test runnable. */
    private static final IgniteRunnable TEST_RUNNABLE = new IgniteRunnable() {
        @Override public void run() {
            //No-op.
        }
    };

    /** Test closure. */
    private static final IgniteClosure<Object, Object> TEST_CLOSURE = new IgniteClosure<Object, Object>() {
        @Override public Object apply(Object o) {
            return null;
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<String, String>(TEST_CACHE)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setCacheStoreFactory(new TestStoreFactory("1", "val"))
            );
    }

    /** {@inheritDoc} */
    @Override protected void prepareCluster() throws Exception {
        startGrid(SRV, ALLOW_ALL, false);

        startGrid("srv_2", ALLOW_ALL, false);

        startGrid(CLNT_ALLOWED_WRITE_PROP, ALLOW_ALL, true);
    }

    /** */
    @Test
    public void testComputeOperations() {
        compute().broadcast(
            new TestRunnable() {
                @Override public void run() {
                    ignite.compute().execute(TEST_COMPUTE_TASK, 0);
                    ignite.compute().broadcast(TEST_CALLABLE);
                    ignite.compute().call(TEST_CALLABLE);
                    ignite.compute().run(TEST_RUNNABLE);
                    ignite.compute().apply(TEST_CLOSURE, new Object());
                    ignite.compute().executeAsync(TEST_COMPUTE_TASK, 0).get();
                    ignite.compute().broadcastAsync(TEST_CALLABLE).get();
                    ignite.compute().callAsync(TEST_CALLABLE).get();
                    ignite.compute().runAsync(TEST_RUNNABLE).get();
                    ignite.compute().applyAsync(TEST_CLOSURE, new Object()).get();
                    try {
                        ignite.executorService().invokeAll(singletonList(TEST_CALLABLE));
                        ignite.executorService().invokeAny(singletonList(TEST_CALLABLE));
                        ignite.executorService().submit(TEST_CALLABLE).get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new IgniteException(e);
                    }
                }
            }
        );
    }

    /** */
    @Test
    public void testCacheOperations() {
        compute().broadcast(
            new TestRunnable() {
                @Override public void run() {
                    IgniteCache<String, String> cache = ignite.cache(TEST_CACHE);

                    cache.put("key", "val");
                    cache.putAll(singletonMap("key", "value"));
                    cache.get("key");
                    cache.getAll(Collections.singleton("key"));
                    cache.containsKey("key");
                    cache.remove("key");
                    cache.removeAll(Collections.singleton("key"));
                    cache.clear();
                    cache.replace("key", "value");
                    cache.putIfAbsent("key", "value");
                    cache.getAndPut("key", "value");
                    cache.getAndRemove("key");
                    cache.getAndReplace("key", "value");

                    cache.invoke("key", processor());
                    cache.invokeAll(singleton("key"), processor());
                    cache.invokeAsync("key", processor()).get();
                    cache.invokeAllAsync(singleton("key"), processor()).get();

                    for (Cache.Entry<String, String> entry : cache)
                        log.info(entry.toString());

                    cache.query(new ScanQuery<String, String>()).getAll();
                }
            }
        );
    }

    /** */
    @Test
    public void testDataStreamerOperations() {
        compute().broadcast(
                new TestRunnable() {
                    @Override public void run() {
                        try (IgniteDataStreamer<String, String> s = ignite.dataStreamer(TEST_CACHE)) {
                            s.addData("k", "val");
                            s.addData(singletonMap("key", "val"));
                            s.addData((Map.Entry<String, String>)entry());
                            s.addData(singletonList(entry()));
                        }
                    }
                });
    }

    /** */
    @Test
    public void testTransaction() {
        compute().broadcast(
            new TestRunnable() {
                @Override public void run() {
                    try (Transaction tx = ignite.transactions().txStart()) {
                        ignite.cache(TEST_CACHE).put("key", "transaction_test");
                        ignite.cache(TEST_CACHE).get("key");

                        tx.commit();
                    }
                }
            });
    }

    /** */
    @Test
    public void testDataStructures() {
        compute().broadcast(
            new TestRunnable() {
                @SuppressWarnings("LockAcquiredButNotSafelyReleased")
                @Override public void run() {
                    IgniteQueue<Object> queueTx = ignite.queue("test_queue_tx", 1,
                        new CollectionConfiguration()
                            .setGroupName("test_queue_tx")
                            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
                    queueTx.add(new Object());
                    queueTx.clear();

                    IgniteQueue<Object> queueAtomic = ignite.queue("test_queue_atomic", 1,
                        new CollectionConfiguration()
                            .setGroupName("test_queue_atomic")
                            .setAtomicityMode(CacheAtomicityMode.ATOMIC));
                    queueAtomic.add(new Object());
                    queueAtomic.clear();

                    IgniteSet<Object> set = ignite.set("test_set", new CollectionConfiguration().setGroupName("test_set"));
                    set.add(new Object());
                    set.clear();

                    IgniteAtomicLong atomicLong = ignite.atomicLong("test_atomic_long", 1, true);
                    atomicLong.incrementAndGet();

                    IgniteAtomicSequence atomicSeq = ignite.atomicSequence("test_atomic_seq", 1,
                        true);
                    atomicSeq.incrementAndGet();

                    IgniteAtomicReference<Object> atomicRef = ignite.atomicReference("test_atomic_ref",
                        null, true);
                    atomicRef.compareAndSet(null, new Object());

                    IgniteAtomicStamped<Object, Object> atomicStamped = ignite.atomicStamped("test_atomic_stmp",
                        null, null, true);
                    atomicStamped.compareAndSet(null, new Object(), null, new Object());

                    IgniteCountDownLatch cntDownLatch = ignite.countDownLatch("test_cnt_down_latch", 1,
                        true, true);
                    cntDownLatch.countDown();

                    IgniteSemaphore semaphore = ignite.semaphore("test_semaphore", 1, true,
                        true);
                    semaphore.acquire();
                    semaphore.release();

                    IgniteLock lock = ignite.reentrantLock("test_lock", true, true, true);
                    lock.lock();
                    lock.unlock();
                }
            });
    }

    /** */
    @Test
    public void testBinary() {
        compute().broadcast(
            new TestRunnable() {
                @Override public void run() {
                    ignite.binary().toBinary(new Object());

                    // Test binary objects.
                    ignite.cache(TEST_CACHE).put(0, new Object());
                    BinaryObject obj = (BinaryObject)ignite.cache(TEST_CACHE).withKeepBinary().get(0);
                    obj.toString();
                }
            });
    }

    /** */
    @Test
    public void testAffinity() {
        compute().broadcast(
            new TestRunnable() {
                @Override public void run() {
                    ignite.affinity(TEST_CACHE).partition(new Object());
                }
            });
    }

    /** */
    private IgniteCompute compute() {
        Ignite clnt = grid(CLNT_ALLOWED_WRITE_PROP);

        return clnt.compute(clnt.cluster().forRemotes());
    }

    /** */
    private CacheEntryProcessor<String, String, String> processor() {
        return (entry, o) -> {
            entry.setValue("Val");

            return null;
        };
    }

    /**
     * @return Cache entry for test.
     */
    private T2<String, String> entry() {
        return new T2<>("key", "val");
    }

    /** */
    private abstract static class TestRunnable implements IgniteRunnable {
        /** Ignite. */
        @IgniteInstanceResource
        protected Ignite ignite;
    }
}
