/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Shuffle for embedded execution mode.
 */
public class GridHadoopEmbeddedShuffle extends GridHadoopShuffleAdapter<UUID> {
    /** Hadoop context. */
    private GridHadoopContext ctx;

    /**
     * @param ctx Hadoop context.
     */
    public GridHadoopEmbeddedShuffle(GridHadoopContext ctx) {
        super(ctx.kernalContext().log(GridHadoopEmbeddedShuffle.class));

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        ctx.kernalContext().io().addUserMessageListener(GridTopic.TOPIC_HADOOP,
            new GridBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID nodeId, Object msg) {
                    return onMessageReceived(nodeId, (GridHadoopMessage)msg);
                }
            });
    }

    /** {@inheritDoc} */
    @Override protected GridHadoopShuffleJob<UUID> newJob(GridHadoopJobId jobId) throws GridException {
        GridHadoopMapReducePlan plan = ctx.jobTracker().plan(jobId);

        GridHadoopShuffleJob<UUID> job = new GridHadoopShuffleJob<>(ctx.localNodeId(), log,
            ctx.jobTracker().job(jobId), mem, plan.reducers(), !F.isEmpty(plan.mappers(ctx.localNodeId())));

        UUID[] rdcAddrs = new UUID[plan.reducers()];

        for (int i = 0; i < rdcAddrs.length; i++) {
            UUID nodeId = plan.nodeForReducer(i);

            assert nodeId != null : "Plan missing node for reducer [plan=" + plan + ", rdc=" + i + ']';

            rdcAddrs[i] = nodeId;
        }

        boolean init = job.initializeReduceAddresses(rdcAddrs);

        assert init;

        return job;
    }

    /** {@inheritDoc} */
    @Override protected void send0(UUID nodeId, Object msg)  {
        GridNode node = ctx.kernalContext().discovery().node(nodeId);

        try {
            ctx.kernalContext().io().sendUserMessage(F.asList(node), msg, GridTopic.TOPIC_HADOOP, false, 0);
        }
        catch (GridException e) {
            U.error(log, "Failed to send message.", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected String gridName() {
        return ctx.kernalContext().gridName();
    }
}
