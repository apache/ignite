// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.GridDeploymentMode.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceProcessor extends GridProcessorAdapter {
    private GridCacheProjection<GridServiceConfigurationKey, GridServiceConfiguration> cfgCache;

    /**
     * @param ctx Kernal context.
     */
    protected GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        GridConfiguration cfg = ctx.config();

        GridDeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new GridException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
    }

    @Override public void onKernalStart() throws GridException {
        cfgCache = ctx.cache().utilityCache(GridServiceConfigurationKey.class, GridServiceConfiguration.class);

        for (GridServiceConfiguration c : ctx.config().getServiceConfiguration())
            deploy(c);
    }

    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel); // TODO: CODE: implement.
    }

    private void checkDeploy() throws GridRuntimeException {
        GridConfiguration cfg = ctx.config();

        GridDeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED))
            throw new GridRuntimeException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
    }

    private void validate(GridServiceConfiguration cfg) throws GridRuntimeException {
        // TODO: implement.
    }

    public GridFuture<?> deployOnEachNode(String name, GridService svc) {
        return deployMultiple(name, svc, 0, 1);
    }

    public GridFuture<?> deploySingleton(String name, GridService svc) {
        return deployMultiple(name, svc, 1, 1);
    }

    public GridFuture<?> deployMultiple(String name, GridService svc, int totalCnt, int maxPerNodeCnt) {
        GridServiceConfiguration cfg = new GridServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setTotalCount(totalCnt);
        cfg.setMaxPerNodeCount(maxPerNodeCnt);

        return deploy(cfg);
    }

    public GridFuture<?> deploy(GridServiceConfiguration cfg) {
        checkDeploy();

        validate(cfg);

        try {
            cfgCache.putIfAbsent(new GridServiceConfigurationKey(cfg.getName()), cfg);
        }
        catch (GridException e) {
            log.error("Failed to deploy service: " + cfg.getName(), e);

            return new GridFinishedFuture<>(ctx, e);
        }

        return null; // TODO
    }

    public <K> GridFuture<UUID> deployForAffinityKey(GridService svc, String cacheName, K affKey) {
        checkDeploy();

        return null; // TODO
    }

    public GridFuture<?> cancel(String name) {
        return null; // TODO
    }

    public Collection<? extends GridServiceDescriptor> deployedServices() {
        return null; // TODO
    }
}
