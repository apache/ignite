/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * {@link GridProduct} implementation.
 */
public class GridProductImpl implements GridProduct, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Copyright blurb. */
    public static final String COPYRIGHT = "2014 Copyright (C) GridGain Systems";

    /** Enterprise edition flag. */
    public static final boolean ENT;

    /** GridGain version. */
    public static final String VER;

    /** Build timestamp in seconds. */
    public static final long BUILD_TSTAMP;

    /** Formatted build date. */
    public static final String BUILD_TSTAMP_STR;

    /** Revision hash. */
    public static final String REV_HASH;

    /** Release date. */
    public static final String RELEASE_DATE;

    /** GridGain version as numeric array (generated from {@link #VER}). */
    public static final byte[] VER_BYTES;

    /** Compound version. */
    public static final String COMPOUND_VER;

    /** Compound version. */
    public static final String ACK_VER;

    /** */
    private GridKernalContext ctx;

    /** */
    private GridProductVersion ver;

    /** Update notifier. */
    private GridUpdateNotifier verChecker;

    /**
     *
     */
    static {
        boolean ent0;

        try {
            ent0 = Class.forName("org.gridgain.grid.kernal.breadcrumb") != null;
        }
        catch (ClassNotFoundException ignored) {
            ent0 = false;
        }

        ENT = ent0;

        VER = GridProperties.get("gridgain.version");
        BUILD_TSTAMP = Long.valueOf(GridProperties.get("gridgain.build"));
        REV_HASH = GridProperties.get("gridgain.revision");
        RELEASE_DATE = GridProperties.get("gridgain.rel.date");

        VER_BYTES = U.intToBytes(VER.hashCode());

        COMPOUND_VER = VER + "-" + (ENT ? "ent" : "os");

        BUILD_TSTAMP_STR = new SimpleDateFormat("yyyyMMdd").format(new Date(BUILD_TSTAMP * 1000));

        String rev = REV_HASH.length() > 8 ? REV_HASH.substring(0, 8) : REV_HASH;

        ACK_VER = COMPOUND_VER + '#' + BUILD_TSTAMP_STR + "-sha1:" + rev;
    }

    /**
     * Required by {@link Externalizable}.
     */
    public GridProductImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param verChecker Update notifier.
     */
    public GridProductImpl(GridKernalContext ctx, GridUpdateNotifier verChecker) {
        this.ctx = ctx;
        this.verChecker = verChecker;

        String releaseType = ctx.isEnterprise() ? "ent" : "os";

        ver = GridProductVersion.fromString(VER + '-' + releaseType + '-' + BUILD_TSTAMP + '-' + REV_HASH);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridProductLicense license() {
        ctx.gateway().readLock();

        try {
            return ctx.license().license();
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void updateLicense(String lic) throws GridProductLicenseException {
        ctx.gateway().readLock();

        try {
            ctx.license().updateLicense(lic);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProductVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return COPYRIGHT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String latestVersion() {
        ctx.gateway().readLock();

        try {
            return verChecker != null ? verChecker.latestVersion() : null;
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridKernalContext)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return ctx.product();
    }
}
