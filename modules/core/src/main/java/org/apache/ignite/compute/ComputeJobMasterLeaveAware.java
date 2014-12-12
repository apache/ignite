package org.apache.ignite.compute;

import org.apache.ignite.*;

/**
 * Annotation for handling master node leave during job execution.
 * <p>
 * If {@link ComputeJob} concrete class implements this interface then in case when master node leaves topology
 * during job execution the callback method {@link #onMasterNodeLeft(ComputeTaskSession)} will be executed.
 * <p>
 * Implementing this interface gives you ability to preserve job execution result or its intermediate state
 * which could be reused later. E.g. you can save job execution result to the database or as a checkpoint
 * and reuse it when failed task is being executed again thus avoiding job execution from scratch.
 */
public interface ComputeJobMasterLeaveAware {
    /**
     * A method which is executed in case master node has left topology during job execution.
     *
     * @param ses Task session, can be used for checkpoint saving.
     * @throws IgniteCheckedException In case of any exception.
     */
    public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteCheckedException;
}
