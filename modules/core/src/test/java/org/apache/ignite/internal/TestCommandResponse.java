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

package org.apache.ignite.internal;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteNodeFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_21_0;
import org.jetbrains.annotations.Nullable;

/** */
@FeatureRegistry(TestIgniteReleaseFeatures_2_21_0.class)
public class TestCommandResponse extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    public @Nullable IgniteNodeFeatureSet jobFeatures;

    /** */
    @Order(1)
    public @Nullable IgniteNodeFeatureSet taskFeatures;

    /** */
    @Order(2)
    public String fldA;

    /** */
    @Order(3)
    public String fldB;

    /** */
    @Order(value = 4, deprecatedBy = "VER_2_21_0_ID_5_FEATURE")
    public String fldC;

    /** */
    @Order(value = 5, introducedBy = "VER_2_21_0_ID_5_FEATURE")
    public String fldD;

    /** */
    public TestCommandResponse() {
        // No-op.
    }

    /** */
    public TestCommandResponse(@Nullable IgniteNodeFeatureSet jobFeatures, TestCommandArgument arg, String fldC, String fldD) {
        this.jobFeatures = jobFeatures;
        this.fldA = arg.fldA;
        this.fldB = arg.fldB;
        this.fldC = fldC;
        this.fldD = fldD;
    }
}
