/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.license;

import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.product.*;

/**
 * License processor.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridLicenseProcessor extends GridProcessor {
    /**
     * Upload the new license into the current node. Throw the exception if the license is not validated.
     *
     * @param licTxt String - The string representation of the license file.
     * @throws GridProductLicenseException - Throw the exception in the case of failed validation.
     */
    public void updateLicense(String licTxt) throws GridProductLicenseException;

    /**
     * Acks the license to the log.
     */
    public void ackLicense();

    /**
     * This method is called periodically by the GridGain to check the license
     * conformance.
     *
     * @throws GridProductLicenseException Thrown in case of any license violation.
     */
    public void checkLicense() throws GridProductLicenseException;

    /**
     * Checks if edition is enabled.
     *
     * @param ed Edition to check.
     * @return {@code True} if enabled.
     */
    public boolean enabled(GridProductEdition ed);

    /**
     * Gets license descriptor.
     *
     * @return License descriptor.
     */
    public GridProductLicense license();

    /**
     * @return Grace period left in minutes if bursting or {@code -1} otherwise.
     */
    public long gracePeriodLeft();
}
