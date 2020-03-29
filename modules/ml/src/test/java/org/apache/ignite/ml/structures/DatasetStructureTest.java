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
            new String[] {"tests"}).data());

        Dataset<DatasetRow<Vector>> dataset = new Dataset<DatasetRow<Vector>>(new DatasetRow[] {},
            new FeatureMetadata[] {});

        Assert.assertEquals("Expect empty data", 0, dataset.data().length);
        Assert.assertEquals("Expect empty meta", 0, dataset.data().length);

        dataset.setData(new DatasetRow[] {new DatasetRow()});
        dataset.setMeta(new FeatureMetadata[] {new FeatureMetadata()});

        Assert.assertEquals("Expect non empty data", 1, dataset.data().length);
        Assert.assertEquals("Expect non empty meta", 1, dataset.data().length);
        Assert.assertEquals(1, dataset.meta().length);
    }
}
