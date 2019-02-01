/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.persistence.scenarios.legacy;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.compatibility.Since;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLegacyNodeScenario;

import static org.apache.ignite.compatibility.persistence.scenarios.util.PdsCompatibilityWithPKIndexMigrationUtils.assertDontUsingPkIndex;
import static org.apache.ignite.compatibility.persistence.scenarios.util.PdsCompatibilityWithPKIndexMigrationUtils.initializeTable;

/**
 * Scenario for checking PK index migration.
 */
@Since("2.4.0")
public class LegacyNodePKIndexMigrationScenarioImpl implements PdsCompatibilityLegacyNodeScenario {
    /** Table name. */
    private final String tblName;

    /**
     * Creates an instance of LegacyNodePKIndexMigrationScenarioImpl.
     *
     * @param tblName Table name.
     */
    public LegacyNodePKIndexMigrationScenarioImpl(String tblName) {
        this.tblName = tblName;
    }

    /** {@inheritDoc} */
    @Override public void run(Ignite ignite) {
        IgniteEx igniteEx = (IgniteEx) ignite;

        initializeTable(igniteEx, tblName);

        assertDontUsingPkIndex(igniteEx, tblName);
    }
}
