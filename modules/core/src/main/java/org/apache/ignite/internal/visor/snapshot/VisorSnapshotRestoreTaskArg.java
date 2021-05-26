package org.apache.ignite.internal.visor.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Argument for the task to manage snapshot restore operation.
 */
public class VisorSnapshotRestoreTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Snapshot name.
     */
    private String snpName;

    /** Cache group names. */
    private Collection<String> grpNames;

    /** Snapshot restore operation management action. */
    private RestoreJobAction action;

    /** Default constructor. */
    public VisorSnapshotRestoreTaskArg() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name.
     * @param grpNames Cache group names.
     * @param action Snapshot restore operation management action.
     */
    public VisorSnapshotRestoreTaskArg(String snpName, @Nullable Collection<String> grpNames, String action) {
        this.snpName = snpName;
        this.grpNames = grpNames;
        this.action = RestoreJobAction.valueOf(action);
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** @return Cache group names. */
    public Collection<String> groupNames() {
        return grpNames;
    }

    /** @return Snapshot restore operation management action. */
    public RestoreJobAction jobAction() {
        return action;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, action);
        U.writeString(out, snpName);
        U.writeCollection(out, grpNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        action = U.readEnum(in, RestoreJobAction.class);
        snpName = U.readString(in);
        grpNames = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSnapshotRestoreTaskArg.class, this);
    }

    /** Snapshot restore operation management action. */
    protected enum RestoreJobAction {
        /** Start snapshot restore operation. */
        START,

        /** Cancel snapshot restore operation. */
        CANCEL,

        /** Status of the snapshot restore operation. */
        STATUS
    }
}
