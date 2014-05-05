/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.config.spring;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.config.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.beans.*;
import org.springframework.context.*;

import java.net.*;
import java.util.*;

/**
 * Spring configuration processor.
 */
public class GridSpringConfigurationProcessor extends GridConfigurationProcessor {
    /** {@inheritDoc} */
    @Override public GridBiTuple<Collection<GridConfiguration>, ? extends GridResourceContext> loadConfigurations(URL cfgUrl)
        throws GridException {
        ApplicationContext springCtx;

        try {
            springCtx = U.applicationContext(cfgUrl);
        }
        catch (BeansException e) {
            throw new GridException("Failed to instantiate Spring XML application context [springUrl=" +
                cfgUrl + ", err=" + e.getMessage() + ']', e);
        }

        Map<String, GridConfiguration> cfgMap;

        try {
            cfgMap = springCtx.getBeansOfType(GridConfiguration.class);
        }
        catch (BeansException e) {
            throw new GridException("Failed to instantiate bean [type=" + GridConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null || cfgMap.isEmpty())
            throw new GridException("Failed to find grid configuration in: " + cfgUrl);

        return F.t(cfgMap.values(), new GridSpringResourceContext(springCtx));
    }
}
