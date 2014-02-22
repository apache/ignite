// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.compute.continuousmapper;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * This task demonstrates how continuous mapper is used. The passed in phrase
 * is split into multiple words and next word is sent out for processing only
 * when the result for the previous word was received.
 * <p>
 * Note that annotation {@link org.gridgain.grid.compute.GridComputeTaskNoResultCache} is optional and tells GridGain
 * not to accumulate results from individual jobs. In this example we increment
 * total character count directly in {@link #result(org.gridgain.grid.compute.GridComputeJobResult, List)} method,
 * and therefore don't need to accumulate them be be processed at reduction step.
 *
 * @author @java.author
 * @version @java.version
 */
@GridComputeTaskNoResultCache
public class GridContinuousMapperTask extends GridComputeTaskAdapter<String, Integer> {
    /** This field will be injected with task continuous mapper. */
    @GridTaskContinuousMapperResource
    private GridComputeTaskContinuousMapper mapper;

    /** Word queue. */
    private final Queue<String> words = new ConcurrentLinkedQueue<>();

    /** Total character count. */
    private final AtomicInteger totalChrCnt = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> grid, String phrase) throws GridException {
        if (phrase == null || phrase.isEmpty())
            throw new GridException("Phrase is empty.");

        // Populate word queue.
        Collections.addAll(words, phrase.split(" "));

        // Sends first word.
        sendWord();

        // Since we have sent at least one job, we are allowed to return
        // 'null' from map method.
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        // If there is an error, fail-over to another node.
        if (res.getException() != null)
            return super.result(res, rcvd);

        // Add result to total character count.
        totalChrCnt.addAndGet(res.<Integer>getData());

        sendWord();

        // If next word was sent, keep waiting, otherwise work queue is empty and we reduce.
        return GridComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
        return totalChrCnt.get();
    }

    /**
     * Sends next queued word to the next node implicitly selected by load balancer.
     *
     * @throws GridException If sending of a word failed.
     */
    private void sendWord() throws GridException {
        // Remove first word from the queue.
        String word = words.poll();

        if (word != null) {
            // Map next word.
            mapper.send(new GridComputeJobAdapter(word) {
                @Override public Object execute() {
                    String word = argument(0);

                    int cnt = GridContinuousMapperExample.charCount(word);

                    // Sleep for some time so it will be visually noticeable that
                    // jobs are executed sequentially.
                    try {
                        Thread.sleep(2000);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }

                    return cnt;
                }
            });
        }
    }
}
