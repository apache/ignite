package org.apache.ignite.console.agent.task;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.jetbrains.annotations.NotNull;

import io.vertx.core.json.JsonObject;

/**
 * This task demonstrates how continuous mapper is used. The passed in phrase
 * is split into multiple words and next word is sent out for processing only
 * when the result for the previous word was received.
 * <p>
 * Note that annotation {@link ComputeTaskNoResultCache} is optional and tells Ignite
 * not to accumulate results from individual jobs. In this example we increment
 * total character count directly in {@link #result(ComputeJobResult, List)} method,
 * and therefore don't need to accumulate them be be processed at reduction step.
 */
@ComputeTaskNoResultCache
public class ContinuousMapperTask extends ComputeTaskAdapter<JsonObject, JsonObject> {
    /** This field will be injected with task continuous mapper. */
    @TaskContinuousMapperResource
    private ComputeTaskContinuousMapper mapper;

    /** Word queue. */
    private final Queue<String> words = new ConcurrentLinkedQueue<>();

    /** Total character count. */
    private final AtomicInteger totalChrCnt = new AtomicInteger(0);

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, JsonObject phrase) {
        if (phrase == null || phrase.isEmpty())
            throw new IgniteException("Phrase is empty.");

        // Populate word queue.
        Collections.addAll(words, phrase.getMap().values().toArray(new String[] {}));

        // Sends first word.
        sendWord();

        // Since we have sent at least one job, we are allowed to return
        // 'null' from map method.
        return null;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        // If there is an error, fail-over to another node.
        if (res.getException() != null)
            return super.result(res, rcvd);

        // Add result to total character count.
        totalChrCnt.addAndGet(res.<Integer>getData());

        sendWord();

        // If next word was sent, keep waiting, otherwise work queue is empty and we reduce.
        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Override public JsonObject reduce(List<ComputeJobResult> results) {        
        
        JsonObject result = new JsonObject();
        result.put("totalChrCnt", totalChrCnt.get());
        return result;
        
    }

    /**
     * Sends next queued word to the next node implicitly selected by load balancer.
     */
    private void sendWord() {
        // Remove first word from the queue.
        String word = words.poll();

        if (word != null) {
            // Map next word.
            mapper.send(new ComputeJobAdapter(word) {
                @Override public Object execute() {
                    String word = argument(0);

                    System.out.println();
                    System.out.println(">>> Printing '" + word + "' from ignite job at time: " + new Date());

                    int cnt = word.length();

                    // Sleep for some time so it will be visually noticeable that
                    // jobs are executed sequentially.
                    try {
                        Thread.sleep(1000);
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
