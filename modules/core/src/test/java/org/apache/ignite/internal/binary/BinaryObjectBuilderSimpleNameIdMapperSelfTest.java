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

package org.apache.ignite.internal.binary;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.binary.BinarySimpleNameIdMapper;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Binary builder test.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class BinaryObjectBuilderSimpleNameIdMapperSelfTest extends BinaryObjectBuilderDefaultIdMapperSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

        bCfg.setIdMapper(new BinarySimpleNameIdMapper());

        // TODO this line must be deleted when IGNITE-2395 will be fixed.
        Collection<BinaryTypeConfiguration> typeCfgs = new ArrayList<>(bCfg.getTypeConfigurations());

        typeCfgs.add(new BinaryTypeConfiguration("Class"));

        bCfg.setTypeConfigurations(typeCfgs);

        return cfg;
    }
}
