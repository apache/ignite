/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Object wrapper containing serialized byte array of original object and deployment information.
 */
class GridAffinityMessage implements Externalizable, IgniteOptimizedMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
    private static Object GG_CLASS_ID;

    /** */
    private byte[] src;

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private IgniteDeploymentMode depMode;

    /** */
    private String srcClsName;

    /** */
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    private Map<UUID, IgniteUuid> ldrParties;

    /**
     * @param src Source object.
     * @param srcClsName Source object class name.
     * @param clsLdrId Class loader ID.
     * @param depMode Deployment mode.
     * @param userVer User version.
     * @param ldrParties Node loader participant map.
     */
    GridAffinityMessage(
        byte[] src,
        String srcClsName,
        IgniteUuid clsLdrId,
        IgniteDeploymentMode depMode,
        String userVer,
        Map<UUID, IgniteUuid> ldrParties) {
        this.src = src;
        this.srcClsName = srcClsName;
        this.depMode = depMode;
        this.clsLdrId = clsLdrId;
        this.userVer = userVer;
        this.ldrParties = ldrParties;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridAffinityMessage() {
        // No-op.
    }

    /**
     * @return Source object.
     */
    public byte[] source() {
        return src;
    }

    /**
     * @return the Class loader ID.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    public IgniteDeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Source message class name.
     */
    public String sourceClassName() {
        return srcClsName;
    }

    /**
     * @return User version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participant map.
     */
    public Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /** {@inheritDoc} */
    @Override public Object ggClassId() {
        return GG_CLASS_ID;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, src);

        out.writeInt(depMode.ordinal());

        U.writeGridUuid(out, clsLdrId);
        U.writeString(out, srcClsName);
        U.writeString(out, userVer);
        U.writeMap(out, ldrParties);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        src = U.readByteArray(in);

        depMode = IgniteDeploymentMode.fromOrdinal(in.readInt());

        clsLdrId = U.readGridUuid(in);
        srcClsName = U.readString(in);
        userVer = U.readString(in);
        ldrParties = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAffinityMessage.class, this);
    }
}
