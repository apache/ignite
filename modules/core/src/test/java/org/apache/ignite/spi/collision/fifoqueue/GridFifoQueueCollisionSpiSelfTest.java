/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.collision.fifoqueue;

import org.apache.ignite.spi.collision.*;
import org.gridgain.grid.util.typedef.CI1;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Unit tests for {@link FifoQueueCollisionSpi}.
 */
@GridSpiTest(spi = FifoQueueCollisionSpi.class, group = "Collision SPI")
public class GridFifoQueueCollisionSpiSelfTest extends GridSpiAbstractTest<FifoQueueCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testCollision0() throws Exception {
        GridCollisionTestContext cntx = createContext(15, 20);

        Collection<CollisionJobContext> activeJobs = cntx.activeJobs();
        Collection<CollisionJobContext> passiveJobs = cntx.waitingJobs();

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : passiveJobs) {
            assert ((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        int i = 0;

        for (CollisionJobContext ctx : activeJobs) {
            if (i++ < 15)
                assert !((GridTestCollisionJobContext)ctx).isActivated();
            else
                assert ((GridTestCollisionJobContext)ctx).isActivated();

            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision1() throws Exception {

        getSpi().setParallelJobsNumber(32);

        GridCollisionTestContext cntx = createContext(0, 33);

        Collection<CollisionJobContext> activeJobs = cntx.activeJobs();
        Collection<CollisionJobContext> passiveJobs = cntx.waitingJobs();

        getSpi().onCollision(cntx);

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionJobContext)ctx).getIndex() == 32) {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
            else {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
        }

        assert activeJobs.size() == 32;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision2() throws Exception {
        getSpi().setParallelJobsNumber(3);

        GridCollisionTestContext cntx = createContext(11, 0);

        Collection<CollisionJobContext> activeJobs = cntx.activeJobs();
        Collection<CollisionJobContext> passiveJobs = cntx.waitingJobs();

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        for (CollisionJobContext ctx : activeJobs) {
            assert !((GridTestCollisionJobContext)ctx).isActivated();
            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        assert passiveJobs.isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollision3() throws Exception {
        getSpi().setParallelJobsNumber(15);

        GridCollisionTestContext cntx = createContext(10, 10);

        Collection<CollisionJobContext> activeJobs = cntx.activeJobs();
        Collection<CollisionJobContext> passiveJobs = cntx.waitingJobs();

        getSpi().onCollision(new GridCollisionTestContext(activeJobs, passiveJobs));

        int i = 0;

        for (CollisionJobContext ctx : activeJobs) {
            if (i++ < 10)
                assert !((GridTestCollisionJobContext)ctx).isActivated();
            else
                assert ((GridTestCollisionJobContext)ctx).isActivated();

            assert !((GridTestCollisionJobContext)ctx).isCanceled();
        }

        for (CollisionJobContext ctx : passiveJobs) {
            if (((GridTestCollisionJobContext)ctx).getIndex() < 5) {
                assert ((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
            else {
                assert !((GridTestCollisionJobContext)ctx).isActivated();
                assert !((GridTestCollisionJobContext)ctx).isCanceled();
            }
        }
    }

    /**
     * Prepare test context.
     *
     * @param activeNum Number of active jobs in context.
     * @param passiveNum Number of passive jobs in context.
     * @return New context with the given numbers of jobs, which will update when one of jobs get activated.
     */
    private GridCollisionTestContext createContext(int activeNum, int passiveNum) {
        final Collection<CollisionJobContext> activeJobs = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < activeNum; i++)
            activeJobs.add(new GridTestCollisionJobContext(new GridTestCollisionTaskSession(), i));

        final Collection<CollisionJobContext> passiveJobs = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < passiveNum; i++)
            passiveJobs.add(new GridTestCollisionJobContext(new GridTestCollisionTaskSession(), i,
                new CI1<GridTestCollisionJobContext>() {
                    @Override public void apply(GridTestCollisionJobContext c) {
                        passiveJobs.remove(c);
                        activeJobs.add(c);
                    }
                }));

        return new GridCollisionTestContext(activeJobs, passiveJobs);
    }
}
