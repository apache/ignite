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
package org.apache.ignite.internal.processors.hadoop.impl.examples;

import java.io.IOException;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopErrorSimulator;

/**
 * Combiner function with pluggable error simulator.
 */
public class HadoopWordCount2Combiner extends HadoopWordCount2Reducer {
    /** {@inheritDoc} */
    @Override protected void configError() {
        HadoopErrorSimulator.instance().onCombineConfigure();
    }

    /** {@inheritDoc} */
    @Override protected void setupError() throws IOException, InterruptedException {
        HadoopErrorSimulator.instance().onCombineSetup();
    }

    /** {@inheritDoc} */
    @Override protected void reduceError() throws IOException, InterruptedException {
        HadoopErrorSimulator.instance().onCombine();
    }

    /** {@inheritDoc} */
    @Override protected void cleanupError() throws IOException, InterruptedException {
        HadoopErrorSimulator.instance().onCombineCleanup();
    }
}