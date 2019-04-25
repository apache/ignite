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

package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionBackupFilterAbstractSelfTest;

/**
 * Partitioned affinity test.
 */
public class RendezvousAffinityFunctionBackupFilterSelfTest extends AffinityFunctionBackupFilterAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunction() {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setBackupFilter(backupFilter);

        return aff;
    }

    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunctionWithAffinityBackupFilter(String attributeName) {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setAffinityBackupFilter(affinityBackupFilter);

        return aff;
    }
}
