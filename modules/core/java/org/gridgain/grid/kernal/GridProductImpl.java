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
import org.jetbrains.annotations.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * {@link GridProduct} implementation.
 */
public class GridProductImpl implements GridProduct {
    /** Ant-augmented version number. */
    static final String VER = /*@java.version*/"x.x.x";

    /** Ant-augmented build number. */
    static final long BUILD = /*@java.build*/0;

    /** Ant-augmented revision hash. */
    static final String REV_HASH = /*@java.revision*/"DEV";

    /** Ant-augmented copyright blurb. */
    static final String COPYRIGHT = /*@java.copyright*/"Copyright (C) 2013 GridGain Systems";

    /** Ant-augmented edition name. */
    private static final String EDITION = /*@java.edition*/"dev";

    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridProductVersion ver;

    /** */
    private final GridProductEdition edition;

    /** Update notifier. */
    private final GridUpdateNotifier verChecker;

    /**
     * @param ctx Kernal context.
     * @param verChecker Update notifier.
     */
    public GridProductImpl(GridKernalContext ctx, GridUpdateNotifier verChecker) {
        this.ctx = ctx;
        this.verChecker = verChecker;

        ver = GridProductVersion.fromString(VER + '-' + BUILD + '-' + REV_HASH);

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
            case "dev":
                return PLATFORM;

            case "datagrid-ent":
            case "datagrid-os":
                return DATA_GRID;

            case "hadoop-ent":
            case "hadoop-os":
                return HADOOP;

            case "streaming-ent":
            case "streaming-os":
                return STREAMING;

            case "mongo-ent":
            case "mongo-os":
                return MONGO;

            case "platform-ent":
            case "platform-os":
                return PLATFORM;
        }

        throw new GridRuntimeException("Failed to determine GridGain edition: " + edition);
    }
}
