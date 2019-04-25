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

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

/**
 * Converts a row of values to actual key+value using {@link UpdatePlan#processRow(List)}.
 */
public class DmlBulkLoadDataConverter extends IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> {
    /** Update plan to convert incoming rows. */
    private final UpdatePlan plan;

    /**
     * Creates the converter with the given update plan.
     *
     * @param plan The update plan to use.
     */
    public DmlBulkLoadDataConverter(UpdatePlan plan) {
        this.plan = plan;
    }

    /**
     * Converts the record to a key+value.
     *
     * @param record The record to convert.
     * @return The key+value.
     * @throws IgniteCheckedException If conversion failed for some reason.
     */
    @Override public IgniteBiTuple<?, ?> applyx(List<?> record) throws IgniteCheckedException {
        return plan.processRow(record);
    }
}
