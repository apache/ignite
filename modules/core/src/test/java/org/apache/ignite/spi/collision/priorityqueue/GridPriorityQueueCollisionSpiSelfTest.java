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

package org.apache.ignite.spi.collision.priorityqueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.GridTestJobContext;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.GridCollisionTestContext;
import org.apache.ignite.spi.collision.GridTestCollisionJobContext;
import org.apache.ignite.spi.collision.GridTestCollisionTaskSession;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi.DFLT_JOB_PRIORITY_ATTRIBUTE_KEY;
import static org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi.DFLT_PARALLEL_JOBS_NUM;
import static org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi.DFLT_PRIORITY_ATTRIBUTE_KEY;
import static org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi.DFLT_STARVATION_INCREMENT;

/**
 * Priority queue collision SPI test.
 */
@GridSpiTest(spi = PriorityQueueCollisionSpi.class, group = "Collision SPI")
public class GridPriorityQueueCollisionSpiSelfTest extends GridSpiAbstractTest<PriorityQueueCollisionSpi> {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        getSpi().setParallelJobsNumber(DFLT_PARALLEL_JOBS_NUM);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testCollisionAttributeName() throws Exception {
        String taskAttrKey = "testTaskPriority";
        String jobAttrKey = "testJobPriority";

        List<CollisionJobContext> activeJobs = makeContextList(taskAttrKey);
        List<CollisionJobContext> passiveJobs = makeContextList(taskAttrKey);

        getSpi().setParallelJobsNumber(12);
        getSpi().setPriorityAttributeKey(taskAttrKey);
        getSpi().setJobPriorityAttributeKey(jobAttrKey);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        int incVal = getSpi().getStarvationIncrement();

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority() >= 8) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();

            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }

            Integer p = ctx.getJobContext().getAttribute(jobAttrKey);

            if (p != null)
                assert p == incVal + ((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority();
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        getSpi().setPriorityAttributeKey(DFLT_PRIORITY_ATTRIBUTE_KEY);
        getSpi().setJobPriorityAttributeKey(DFLT_JOB_PRIORITY_ATTRIBUTE_KEY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        getSpi().setParallelJobsNumber(20);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            assert ((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision0() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        getSpi().setParallelJobsNumber(30);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            assert ((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public void testCollision1() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        getSpi().setParallelJobsNumber(12);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        int incVal = getSpi().getStarvationIncrement();

        String jobAttrKey = DFLT_JOB_PRIORITY_ATTRIBUTE_KEY;

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority() >= 8) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();

            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }

            Integer p = ctx.getJobContext().<String, Integer>getAttribute(jobAttrKey);

            if (p != null)
                assert p == incVal + ((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority();
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision2() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        getSpi().setParallelJobsNumber(10);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision3() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        getSpi().setParallelJobsNumber(5);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisionEmpty() throws Exception {
        Collection<CollisionJobContext> activeJobs = new ArrayList<>();
        Collection<CollisionJobContext> passiveJobs = new ArrayList<>();

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        assert activeJobs.isEmpty();
        assert passiveJobs.isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisionWithoutPriorityAttribute() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority() >= 8) {
                ((GridTestCollisionTaskSession)ctx.getTaskSession()).setPriorityAttributeKey("bad-attr-name");

                ((GridTestCollisionJobContext)ctx).setJobContext(new GridTestJobContext() {
                    @SuppressWarnings({"unchecked", "RedundantTypeArguments"})
                    @Override public <K, V> V getAttribute(K key) {
                        if (DFLT_JOB_PRIORITY_ATTRIBUTE_KEY.equals(key))
                            return null;

                        return super.<K, V>getAttribute(key);
                    }
                });
            }
        }

        getSpi().setParallelJobsNumber(12);
        getSpi().setDefaultPriority(100);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority() >= 8) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();

            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisionWithWrongPriorityAttribute() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null);
        List<CollisionJobContext> passiveJobs = makeContextList(null);

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority() >= 8) {
                ((GridTestCollisionJobContext)ctx).setTaskSession(new GridTestCollisionTaskSession(100,
                    DFLT_PRIORITY_ATTRIBUTE_KEY) {
                    @SuppressWarnings("unchecked")
                    @Override public <K, V> V getAttribute(K key) {
                        if (getPriorityAttributeKey() != null && getPriorityAttributeKey().equals(key))
                            return (V)"wrong-attr";

                        return null;
                    }
                });

                ((GridTestCollisionJobContext)ctx).setJobContext(new GridTestJobContext() {
                    @SuppressWarnings({"unchecked", "RedundantTypeArguments"})
                    @Override public <K, V> V getAttribute(K key) {
                        if (DFLT_JOB_PRIORITY_ATTRIBUTE_KEY.equals(key))
                            return (V)"wrong-attr";

                        return super.<K, V>getAttribute(key);
                    }
                });
            }
        }

        getSpi().setParallelJobsNumber(12);
        getSpi().setDefaultPriority(100);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority() >= 8) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();

            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testCollision4() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null, false);
        List<CollisionJobContext> passiveJobs = makeContextList(null, false);

        int incVal = 2;

        getSpi().setParallelJobsNumber(12);
        getSpi().setStarvationIncrement(incVal);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        String jobAttrKey = DFLT_JOB_PRIORITY_ATTRIBUTE_KEY;

        for (CollisionJobContext ctx : passiveJobs) {
            int taskP = ((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority();

            if (taskP >= 8) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }

            if (taskP < 5) {
                Integer p = ctx.getJobContext().getAttribute(jobAttrKey);

                assert p != null;

                assert p == incVal + taskP;
            }
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        getSpi().setStarvationIncrement(DFLT_STARVATION_INCREMENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision5() throws Exception {
        List<CollisionJobContext> activeJobs = makeContextList(null, false);
        List<CollisionJobContext> passiveJobs = makeContextList(null, false);

        getSpi().setParallelJobsNumber(12);
        getSpi().setStarvationPreventionEnabled(false);

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        String jobAttrKey = DFLT_JOB_PRIORITY_ATTRIBUTE_KEY;

        for (CollisionJobContext ctx : passiveJobs) {
            int taskP = ((GridTestCollisionTaskSession)ctx.getTaskSession()).getPriority();

            if (taskP >= 8) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }

            assert ctx.getJobContext().<String, Integer>getAttribute(jobAttrKey) == null;
        }

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @param attrKey Attribute key,
     * @param shuffle Whether result list should be shuffle.
     * @return List of job collision contexts.
     */
    private List<CollisionJobContext> makeContextList(@Nullable String attrKey, boolean shuffle) {
        if (attrKey == null)
            attrKey = DFLT_PRIORITY_ATTRIBUTE_KEY;

        List<CollisionJobContext> jobs = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            jobs.add(new GridTestCollisionJobContext(new GridTestCollisionTaskSession(i, attrKey)));

        if (shuffle)
            Collections.shuffle(jobs);

        return jobs;
    }

    /**
     * @param attrKey Attribute key.
     * @return List of job collision contexts.
     */
    private List<CollisionJobContext> makeContextList(@Nullable String attrKey) {
        return makeContextList(attrKey, true);
    }
}