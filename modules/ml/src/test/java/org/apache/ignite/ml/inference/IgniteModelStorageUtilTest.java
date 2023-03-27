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

package org.apache.ignite.ml.inference;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.plugin.MLPluginConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link IgniteModelStorageUtil}.
 */
public class IgniteModelStorageUtilTest extends GridCommonAbstractTest {
    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /**
     * Constructs a new instance of Ignite model storage util test.
     */
    public IgniteModelStorageUtilTest() {
        cfg = new IgniteConfiguration();

        MLPluginConfiguration mlCfg = new MLPluginConfiguration();
        mlCfg.setWithMdlDescStorage(true);
        mlCfg.setWithMdlStorage(true);

        cfg.setPluginConfigurations(mlCfg);
    }

    /** */
    @Test
    public void testSaveAndGet() throws Exception {
        try (Ignite ignite = startGrid(cfg)) {
            IgniteModelStorageUtil.saveModel(ignite, i -> 0.42, "mdl");
            Model<Vector, Double> infMdl = IgniteModelStorageUtil.getModel(ignite, "mdl");

            assertEquals(0.42, infMdl.predict(VectorUtils.of()));
        }
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testSaveModelWithTheSameName() throws Exception {
        try (Ignite ignite = startGrid(cfg)) {
            IgniteModelStorageUtil.saveModel(ignite, i -> 0.42, "mdl");
            IgniteModelStorageUtil.saveModel(ignite, i -> 0.42, "mdl");
        }
    }

    /** */
    @Test
    public void testSaveRemoveSaveModel() throws Exception {
        try (Ignite ignite = startGrid(cfg)) {
            IgniteModelStorageUtil.saveModel(ignite, i -> 0.42, "mdl");
            IgniteModelStorageUtil.removeModel(ignite, "mdl");
            IgniteModelStorageUtil.saveModel(ignite, i -> 0.43, "mdl");

            Model<Vector, Double> infMdl = IgniteModelStorageUtil.getModel(ignite, "mdl");

            assertEquals(0.43, infMdl.predict(VectorUtils.of()));
        }
    }
}
