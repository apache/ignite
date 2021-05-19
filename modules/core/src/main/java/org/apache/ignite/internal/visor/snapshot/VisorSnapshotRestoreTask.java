package org.apache.ignite.internal.visor.snapshot;

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * @see IgniteSnapshot#restoreSnapshot(String, Collection)
 */
@GridInternal
public class VisorSnapshotRestoreTask extends VisorOneNodeTask<VisorSnapshotRestoreTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotRestoreTaskArg, String> job(VisorSnapshotRestoreTaskArg arg) {
        return new VisorSnapshotCreateJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotCreateJob extends VisorJob<VisorSnapshotRestoreTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCreateJob(VisorSnapshotRestoreTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotRestoreTaskArg arg) throws IgniteException {
            new SnapshotMXBeanImpl(ignite.context()).restoreSnapshot(arg.snapshotName(), arg.groupName());

            return "Snapshot cache group restore operation started [snapshot=" + arg.snapshotName() +
                (arg.groupName() == null ? "" : ", group=" + arg.groupName()) + ']';
        }
    }
}
