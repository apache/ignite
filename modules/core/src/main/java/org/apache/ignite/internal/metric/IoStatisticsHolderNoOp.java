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
 *
 */

package org.apache.ignite.internal.metric;

/**
 * No Operation IO statistics holder. Use in case statistics shouldn't be gathered.
 */
public class IoStatisticsHolderNoOp implements IoStatisticsHolder {
    /** No-op statistics. */
    public static final IoStatisticsHolderNoOp INSTANCE = new IoStatisticsHolderNoOp();

    /**
     * Private constructor.
     */
    private IoStatisticsHolderNoOp() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String metricRegistryName() {
        return null;
    }
}
