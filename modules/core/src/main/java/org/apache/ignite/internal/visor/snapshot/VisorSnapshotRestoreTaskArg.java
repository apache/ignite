package org.apache.ignite.internal.visor.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
    private String grpName;

    /**
     * Default constructor.
     */
    public VisorSnapshotRestoreTaskArg() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name.
     * @param grpName Cache group name.
     */
    public VisorSnapshotRestoreTaskArg(String snpName, @Nullable String grpName) {
        this.snpName = snpName;
        this.grpName = grpName;
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
    public String groupName() {
        return grpName;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, snpName);
        U.writeString(out, grpName);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        snpName = U.readString(in);
        grpName = U.readString(in);
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return S.toString(VisorSnapshotRestoreTaskArg.class, this);
    }
}
