/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision.priorityqueue;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.testframework.junits.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.spi.collision.priorityqueue.GridPriorityQueueCollisionSpi.*;

/**
 * Priority queue collision SPI test.
 */
@GridSpiTest(spi = GridPriorityQueueCollisionSpi.class, group = "Collision SPI")
public class GridPriorityQueueCollisionSpiSelfTest extends GridSpiAbstractTest<GridPriorityQueueCollisionSpi> {
    /** {@inheritDoc} */
    @Override protected void afterTest() {
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
