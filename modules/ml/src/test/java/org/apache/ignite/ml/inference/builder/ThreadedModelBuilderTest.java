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

package org.apache.ignite.ml.inference.builder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.ml.inference.Model;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ThreadedModelBuilder} class.
 */
public class ThreadedModelBuilderTest {
    /** */
    @Test
    public void testBuild() throws ExecutionException, InterruptedException {
        AsyncModelBuilder mdlBuilder = new ThreadedModelBuilder(10);

        Model<Integer, Future<Integer>> infMdl = mdlBuilder.build(
            ModelBuilderTestUtil.getReader(),
            ModelBuilderTestUtil.getParser()
        );

        for (int i = 0; i < 100; i++)
            assertEquals(Integer.valueOf(i), infMdl.predict(i).get());
    }
}
