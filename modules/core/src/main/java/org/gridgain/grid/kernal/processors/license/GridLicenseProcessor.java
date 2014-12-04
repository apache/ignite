/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.license;

import org.apache.ignite.product.*;
import org.gridgain.grid.kernal.processors.*;
import org.jetbrains.annotations.*;

/**
 * License processor.
 */
public interface GridLicenseProcessor extends GridProcessor {
    /**
     * Upload the new license into the current node.
     *
     * @param licTxt The string representation of the license file.
     * @throws org.apache.ignite.product.IgniteProductLicenseException Thrown if validation check failed for specified license or
     *      license can not be updated because configured license URL use non-file scheme.
     * @see org.apache.ignite.configuration.IgniteConfiguration#getLicenseUrl()
     */
    public void updateLicense(String licTxt) throws IgniteProductLicenseException;

    /**
     * Acks the license to the log.
     */
    public void ackLicense();

    /**
     * This method is called periodically by the GridGain to check the license conformance.
     *
     * @throws org.apache.ignite.product.IgniteProductLicenseException Thrown in case of any license violation.
     */
    public void checkLicense() throws IgniteProductLicenseException;

    /**
     * Checks if subsystem is enabled.
     *
     * @param ed Subsystem to check.
     * @return {@code True} if enabled.
     */
    public boolean enabled(GridLicenseSubsystem ed);

    /**
     * Gets license descriptor.
     *
     * @return License descriptor or {@code null} for open-source edition.
     */
    @Nullable public IgniteProductLicense license();

    /**
     * @return Grace period left in minutes if bursting or {@code -1} otherwise.
     */
    public long gracePeriodLeft();
}
