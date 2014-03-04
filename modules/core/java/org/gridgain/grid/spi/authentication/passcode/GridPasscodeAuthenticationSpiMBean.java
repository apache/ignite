/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication.passcode;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management bean for {@link GridPasscodeAuthenticationSpi}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean that provides access to passcode authentication SPI configuration.")
public interface GridPasscodeAuthenticationSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets text presentation of the valid passcodes collection.
     *
     * @return Text presentation of the valid passcodes collection.
     */
    @GridMBeanDescription("String presentation of the valid passcodes collection.")
    public String getPasscodesFormatted();
}
