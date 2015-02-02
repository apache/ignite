/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import org.apache.ignite.lang.*;
import org.apache.ignite.internal.product.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * {@link org.apache.ignite.internal.product.IgniteProduct} implementation.
 */
public class GridProductImpl implements IgniteProduct, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Copyright blurb. */
    public static final String COPYRIGHT = "2015 Copyright(C) Apache Software Foundation";

    /** Enterprise edition flag. */
    public static final boolean ENT;

    /** Ignite version. */
    public static final String VER;

    /** Build timestamp in seconds. */
    public static final long BUILD_TSTAMP;

    /** Formatted build date. */
    public static final String BUILD_TSTAMP_STR;

    /** Revision hash. */
    public static final String REV_HASH;

    /** Release date. */
    public static final String RELEASE_DATE;

    /** Ignite version as numeric array (generated from {@link #VER}). */
    public static final byte[] VER_BYTES;

    /** Compound version. */
    public static final String COMPOUND_VER;

    /** Compound version. */
    public static final String ACK_VER;

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteProductVersion ver;

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

        VER = GridProperties.get("ignite.version");
        BUILD_TSTAMP = Long.valueOf(GridProperties.get("ignite.build"));
        REV_HASH = GridProperties.get("ignite.revision");
        RELEASE_DATE = GridProperties.get("ignite.rel.date");

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

        ver = IgniteProductVersion.fromString(VER + '-' + releaseType + '-' + BUILD_TSTAMP + '-' + REV_HASH);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteProductLicense license() {
        ctx.gateway().readLock();

        try {
            return ctx.license().license();
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void updateLicense(String lic) throws IgniteProductLicenseException {
        ctx.gateway().readLock();

        try {
            ctx.license().updateLicense(lic);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
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
