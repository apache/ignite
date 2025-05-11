/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.web.model;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Web model for period filter request.
 */
public class PeriodFilterRequest {
    /** */
    private long startDate;

    /** */
    private long endDate;

    /**
     * @return Start date.
     */
    public long getStartDate() {
        return startDate;
    }

    /**
     * @param startDate Start date.
     */
    public void setStartDate(long startDate) {
        this.startDate = startDate;
    }

    /**
     * @return End date.
     */
    public long getEndDate() {
        return endDate;
    }

    /**
     * @param endDate End date.
     */
    public void setEndDate(long endDate) {
        this.endDate = endDate;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PeriodFilterRequest.class, this);
    }
}
