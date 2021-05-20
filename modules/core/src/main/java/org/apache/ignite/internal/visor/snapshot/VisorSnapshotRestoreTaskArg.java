package org.apache.ignite.internal.visor.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

public class VisorSnapshotRestoreTaskArg extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     * Snapshot name.
     */
    private String snpName;

    /**
     * Cache group name.
     */
    private Collection<String> grpNames;

    /**
     * Default constructor.
     */
    public VisorSnapshotRestoreTaskArg() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name.
     * @param grpNames Cache group names separated by commas.
     */
    public VisorSnapshotRestoreTaskArg(String snpName, @Nullable Collection<String> grpNames) {
        this.snpName = snpName;
        this.grpNames = grpNames;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Cache group name.
     */
    public Collection<String> groupNames() {
        return grpNames;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, snpName);
        U.writeCollection(out, grpNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        snpName = U.readString(in);
        grpNames = U.readCollection(in);
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return S.toString(VisorSnapshotRestoreTaskArg.class, this);
    }
}
