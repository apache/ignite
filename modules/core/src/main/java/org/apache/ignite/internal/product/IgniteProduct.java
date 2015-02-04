/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.product;

import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

/**
 * Provides information about current release. Note that enterprise users are also
 * able to renew license. Instance of {@code GridProduct} is obtained from grid as follows:
 * <pre name="code" class="java">
 * GridProduct p = Ignition.ignite().product();
 * </pre>
 */
public interface IgniteProduct {
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
     * Gets product version for this release.
     *
     * @return Product version for this release.
     */
    public IgniteProductVersion version();

    /**
     * Copyright statement for GridGain code.
     *
     * @return Legal copyright statement for GridGain code.
     */
    public String copyright();

    /**
     * Gets latest version available for download or
     * {@code null} if information is not available.
     *
     * @return Latest version string or {@code null} if information is not available.
     */
    @Nullable public String latestVersion();
}
