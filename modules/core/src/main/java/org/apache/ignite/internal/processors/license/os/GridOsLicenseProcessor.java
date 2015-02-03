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

package org.apache.ignite.internal.processors.license.os;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.license.*;
import org.apache.ignite.internal.product.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation for {@link GridLicenseProcessor}.
 */
public class GridOsLicenseProcessor extends GridProcessorAdapter implements GridLicenseProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsLicenseProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void updateLicense(String licTxt) throws IgniteProductLicenseException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void ackLicense() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void checkLicense() throws IgniteProductLicenseException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled(GridLicenseSubsystem ed) {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteProductLicense license() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long gracePeriodLeft() {
        return -1;
    }
}
