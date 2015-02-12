package org.apache.ignite.internal;

import org.apache.ignite.internal.product.*;
import org.jetbrains.annotations.*;

/**
 *
 */
public interface GridProduct {
    /**
     * Gets license descriptor for enterprise edition or {@code null} for open source edition.
     *
     * @return License descriptor.
     */
    @Nullable public IgniteProductLicense license();

    /**
     * Updates to a new license in enterprise edition. This method is no-op in open source edition.
     *
     * @param lic The content of the license.
     * @throws IgniteProductLicenseException If license could not be updated.
     */
    public void updateLicense(String lic) throws IgniteProductLicenseException;

    /**
     * @return Grace period left.
     */
    public long gracePeriodLeft();
}
