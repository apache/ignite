/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.deployment;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

/**
 * Test class deployment.
 */
public class GridTestDeployment extends GridDeployment {
    /**
     * @param depMode Deployment mode.
     * @param clsLdr Class loader.
     * @param clsLdrId Class loader ID.
     * @param userVer User version.
     * @param sampleClsName Sample class name.
     * @param loc {@code True} if local deployment.
     */
    public GridTestDeployment(GridDeploymentMode depMode, ClassLoader clsLdr, IgniteUuid clsLdrId,
        String userVer, String sampleClsName, boolean loc) {
        super(depMode, clsLdr, clsLdrId, userVer, sampleClsName, loc);
    }
}
