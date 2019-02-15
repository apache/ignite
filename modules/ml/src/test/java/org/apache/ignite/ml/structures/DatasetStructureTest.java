/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.structures;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link Dataset} basic features.
 */
public class DatasetStructureTest {
    /**
     * Basic test
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testBasic() {
        Assert.assertNull("Feature names constructor", new Dataset<DatasetRow<Vector>>(1, 1,
            new String[] {"tests"}, false).data());

        Dataset<DatasetRow<Vector>> dataset = new Dataset<DatasetRow<Vector>>(new DatasetRow[] {},
            new FeatureMetadata[] {});

        Assert.assertEquals("Expect empty data", 0, dataset.data().length);
        Assert.assertEquals("Expect empty meta", 0, dataset.data().length);
        Assert.assertFalse("Not distributed by default", dataset.isDistributed());

        dataset.setData(new DatasetRow[] {new DatasetRow()});
        dataset.setMeta(new FeatureMetadata[] {new FeatureMetadata()});
        dataset.setDistributed(true);

        Assert.assertEquals("Expect non empty data", 1, dataset.data().length);
        Assert.assertEquals("Expect non empty meta", 1, dataset.data().length);
        Assert.assertTrue("Expect distributed", dataset.isDistributed());
        Assert.assertEquals(1, dataset.meta().length);
    }
}
