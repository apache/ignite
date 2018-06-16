package org.apache.ignite.tensorflow.proc.longrunning.task;

import org.apache.ignite.tensorflow.proc.longrunning.task.util.LongRunningProcessState;
import org.apache.ignite.tensorflow.proc.longrunning.task.util.LongRunningProcessStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Task that clears process metadata on the nodes where the process has run.
 */
public class LongRunningProcessClearTask extends LongRunningProcessTask<List<LongRunningProcessStatus>> {
    /** */
    private static final long serialVersionUID = -1840332865137076107L;

    /** Process identifiers. */
    private final List<UUID> procIds;

    /**
     * Constructs a new instance of clear task.
     *
     * @param procIds Process identifiers.
     */
    public LongRunningProcessClearTask(List<UUID> procIds) {
        assert procIds != null : "Process identifiers should not be null";

        this.procIds = procIds;
    }

    /** {@inheritDoc} */
    @Override public List<LongRunningProcessStatus> call() {
        ArrayList<LongRunningProcessStatus> res = new ArrayList<>();

        for (UUID prodId : procIds)
            res.add(prepareProcessForRemoving(prodId));

        // All-or-nothing strategy. Processes will be removed only if all processes can be removed.
        removeProcessesFromMetadataStorage();

        return res;
    }

    /**
     * Prepares processes to be removed.
     *
     * @param procId Process identifier.
     * @return Process status.
     */
    private LongRunningProcessStatus prepareProcessForRemoving(UUID procId) {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        Future<?> fut = metadataStorage.get(procId);

        if (fut == null)
            return new LongRunningProcessStatus(LongRunningProcessState.NOT_FOUND);

        if (!fut.isDone())
            throw new IllegalStateException("Process is still running [procId=" + procId + "]");

        try {
            fut.get();
            return new LongRunningProcessStatus(LongRunningProcessState.DONE);
        }
        catch (Exception e) {
            return new LongRunningProcessStatus(LongRunningProcessState.DONE, e);
        }
    }

    /**
     * Removes processes from metadata storage.
     */
    private void removeProcessesFromMetadataStorage() {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        for (UUID procId : procIds)
            metadataStorage.remove(procId);
    }
}
