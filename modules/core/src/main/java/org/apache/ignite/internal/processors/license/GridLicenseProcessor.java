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

package org.apache.ignite.internal.processors.license;

import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.product.*;
import org.jetbrains.annotations.*;

/**
 * License processor.
 */
public interface GridLicenseProcessor extends GridProcessor {
    /**
     * Upload the new license into the current node.
     *
     * @param licTxt The string representation of the license file.
     * @throws IgniteProductLicenseException Thrown if validation check failed for specified license or
     *      license can not be updated because configured license URL use non-file scheme.
     */
    public void updateLicense(String licTxt) throws IgniteProductLicenseException;

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
