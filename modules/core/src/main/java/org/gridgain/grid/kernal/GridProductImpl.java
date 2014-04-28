/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * {@link GridProduct} implementation.
 */
public class GridProductImpl implements GridProduct {
    /** GridGain version. */
    public static final String VER;

    /** Enterprise release flag. */
    public static final boolean ENT;

    /** Ant-augmented edition name. */
    public static final String EDITION = /*@java.edition*/"platform";

    /** Compound GridGain version. */
    public static final String COMPOUND_VERSION;

    /** GridGain version as numeric array (generated from {@link #VER}). */
    public static final byte[] VER_BYTES;

    /** Ant-augmented copyright blurb. */
    public static final String COPYRIGHT = /*@java.copyright*/"Copyright (C) 2014 GridGain Systems";

    /** Ant-augmented build number. */
    public static final long BUILD = /*@java.build*/0;

    /** Ant-augmented revision hash. */
    public static final String REV_HASH = /*@java.revision*/"DEV";

    /** Ant-augmented release date. */
    public static final String RELEASE_DATE = /*@java.rel.date*/"01011970";

    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridProductVersion ver;

    /** */
    private final GridProductEdition edition;

    /** Update notifier. */
    private final GridUpdateNotifier verChecker;

    /** */
    static {
        boolean ent0;

        try {
            ent0 = Class.forName("org.gridgain.grid.kernal.breadcrumb") != null;
        }
        catch (ClassNotFoundException ignored) {
            ent0 = false;
        }

        ENT = ent0;

        final String propsFile = "gridgain.properties";

        Properties props = new Properties();

        try {
            props.load(GridProductImpl.class.getClassLoader().getResourceAsStream(propsFile));

            String prop = EDITION + ".ver";

            VER = props.getProperty(prop);

            if (F.isEmpty(VER))
                throw new RuntimeException("Cannot read '" + prop + "' property from " + propsFile + " file.");
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot find '" + propsFile + "' file.", e);
        }

        COMPOUND_VERSION = EDITION + "-" + (ENT ? "ent" : "os") + "-" + VER;

        VER_BYTES = U.intToBytes(COMPOUND_VERSION.hashCode());
    }

    /**
     * @param ctx Kernal context.
     * @param verChecker Update notifier.
     */
    public GridProductImpl(GridKernalContext ctx, GridUpdateNotifier verChecker) {
        this.ctx = ctx;
        this.verChecker = verChecker;

        String releaseType = ctx.isEnterprise() ? "ent" : "os";

        ver = GridProductVersion.fromString(EDITION + "-" + releaseType + "-" + VER + '-' + BUILD + '-' + REV_HASH);

        edition = editionFromString(EDITION);
    }

    /** {@inheritDoc} */
    @Override public GridProductEdition edition() {
        return edition;
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

    /**
     * @param edition Edition name.
     * @return Edition.
     */
    private static GridProductEdition editionFromString(String edition) {
        switch (edition) {
            case "datagrid":
                return DATA_GRID;

            case "hadoop":
                return HADOOP;

            case "streaming":
                return STREAMING;

            case "mongo":
                return MONGO;

            case "dev":
            case "platform":
                return PLATFORM;
        }

        throw new GridRuntimeException("Failed to determine GridGain edition: " + edition);
    }
}
