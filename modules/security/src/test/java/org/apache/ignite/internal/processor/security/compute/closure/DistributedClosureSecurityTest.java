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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Testing permissions when the compute closure is executed cache operations on remote node.
 */
public class DistributedClosureSecurityTest extends AbstractComputeTaskSecurityTest {
    /** {@inheritDoc} */
    @Override protected void checkSuccess(IgniteEx initiator, IgniteEx remote) {
        for (TransitionClosure tr : transitions()) {
            successCall(
                remote,
                (val) -> tr.remouteC3().accept(
                    initiator.compute(initiator.cluster().forNode(remote.localNode())), "key", val
                )
            );

            successCall(remote,
                (val) -> tr.accept(
                    initiator.compute(initiator.cluster().forNode(srvTransitionAllPerms.localNode())),
                    remote.localNode().id(), "key", val
                )
            );
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkFail(IgniteEx initiator, IgniteEx remote) {
        for (TransitionClosure tr : transitions()) {
            failCall(
                remote,
                () ->
                    tr.remouteC3().accept(
                        initiator.compute(initiator.cluster().forNode(remote.localNode())), "fail_key", -1
                    )
            );

            failCall(remote,
                () ->
                    tr.accept(
                        initiator.compute(initiator.cluster().forNode(srvTransitionAllPerms.localNode())),
                        remote.localNode().id(), "fail_key", -1
                    )
            );
        }
    }

    /**
     * @param remote Remote node.
     */
    private void successCall(IgniteEx remote, Consumer<Integer> c) {
        int val = values.getAndIncrement();

        c.accept(val);

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param remote Remote node.
     */
    private void failCall(IgniteEx remote, Runnable r) {
        assertCauseSecurityException(
            GridTestUtils.assertThrowsWithCause(r, SecurityException.class)
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /** */
    private Collection<TransitionClosure> transitions() {
        return Arrays.asList(
            //IgniteCompute.broadcast (broadcastAsync)
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.broadcast(
                        new IgniteRunnable() {
                            @Override public void run() {
                                remouteC3().accept(compute(uuid), k, v);
                            }
                        }
                    );
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.broadcast(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);
                                    }
                                }
                            );
                        }
                    };
                }
            },
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.broadcastAsync(
                        new IgniteRunnable() {
                            @Override public void run() {
                                remouteC3().accept(compute(uuid), k, v);
                            }
                        }
                    ).get();
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.broadcastAsync(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);
                                    }
                                }
                            ).get();
                        }
                    };
                }
            },
            //IgniteCompute.call (callAsync)
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.call(
                        new IgniteCallable<Object>() {
                            @Override public Object call() {
                                remouteC3().accept(compute(uuid), k, v);

                                return null;
                            }
                        }
                    );
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.call(
                                new IgniteCallable<Object>() {
                                    @Override public Object call() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                                        return null;
                                    }
                                }
                            );
                        }
                    };
                }
            },
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.callAsync(
                        new IgniteCallable<Object>() {
                            @Override public Object call() {
                                remouteC3().accept(compute(uuid), k, v);

                                return null;
                            }
                        }
                    ).get();
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.callAsync(
                                new IgniteCallable<Object>() {
                                    @Override public Object call() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                                        return null;
                                    }
                                }
                            ).get();
                        }
                    };
                }
            },
            //IgniteCompute.run (runAsync)
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.run(
                        new IgniteRunnable() {
                            @Override public void run() {
                                remouteC3().accept(compute(uuid), k, v);
                            }
                        }
                    );
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.broadcast(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);
                                    }
                                }
                            );
                        }
                    };
                }
            },
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.runAsync(
                        new IgniteRunnable() {
                            @Override public void run() {
                                remouteC3().accept(compute(uuid), k, v);
                            }
                        }
                    ).get();
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.broadcastAsync(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);
                                    }
                                }
                            ).get();
                        }
                    };
                }
            },
            //IgniteCompute.apply (applyAsync)
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.apply(
                        new IgniteClosure<Object, Object>() {
                            @Override public Object apply(Object o) {
                                remouteC3().accept(compute(uuid), k, v);

                                return null;
                            }
                        }, new Object()
                    );
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.apply(
                                new IgniteClosure<Object, Object>() {
                                    @Override public Object apply(Object o) {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                                        return null;
                                    }
                                }, new Object()
                            );
                        }
                    };
                }
            },
            new TransitionClosure() {
                @Override public void accept(IgniteCompute compute, UUID uuid, String k, Integer v) {
                    compute.applyAsync(
                        new IgniteClosure<Object, Object>() {
                            @Override public Object apply(Object o) {
                                remouteC3().accept(compute(uuid), k, v);

                                return null;
                            }
                        }, new Object()
                    ).get();
                }

                @Override protected C3<IgniteCompute, String, Integer> remouteC3() {
                    return new C3<IgniteCompute, String, Integer>() {
                        @Override public void accept(IgniteCompute compute, String k, Integer v) {
                            compute.applyAsync(
                                new IgniteClosure<Object, Object>() {
                                    @Override public Object apply(Object o) {
                                        Ignition.localIgnite().cache(CACHE_NAME).put(k, v);

                                        return null;
                                    }
                                }, new Object()
                            ).get();
                        }
                    };
                }
            }
        );
    }

    /**
     * Closure for transition tests.
     */
    private abstract static class TransitionClosure {
        /**
         * @return IgniteCompute for group that contains only remoteId node.
         */
        protected IgniteCompute compute(UUID remoteId) {
            IgniteEx loc = (IgniteEx)Ignition.localIgnite();

            return loc.compute(loc.cluster().forNode(
                loc.cluster().node(remoteId)
            ));
        }

        /**
         * Performs this operation on the given arguments.
         */
        public abstract void accept(IgniteCompute cmpt, UUID id, String k, Integer v);

        /**
         * @return TriConsumer.
         */
        protected abstract C3<IgniteCompute, String, Integer> remouteC3();
    }
}
