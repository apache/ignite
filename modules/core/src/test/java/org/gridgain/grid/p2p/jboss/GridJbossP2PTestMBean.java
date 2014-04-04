/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p.jboss;

import org.jboss.system.*;

/**
 *
 * JBoss P2P test MBean.
 */
public interface GridJbossP2PTestMBean extends ServiceMBean {
    /** */
    public void doubleDeploymentSelfTest();

    /** */
    public void hotRedeploymentSelfTest();

    /** */
    public void uploadSelfTest();
}
