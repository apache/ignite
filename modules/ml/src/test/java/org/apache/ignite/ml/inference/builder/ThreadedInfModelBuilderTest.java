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

package org.apache.ignite.ml.inference.builder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.ml.inference.InfModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ThreadedInfModelBuilder} class.
 */
public class ThreadedInfModelBuilderTest {
    /** */
    @Test
    public void testBuild() throws ExecutionException, InterruptedException {
        AsyncInfModelBuilder mdlBuilder = new ThreadedInfModelBuilder(10);

        InfModel<Integer, Future<Integer>> infMdl = mdlBuilder.build(
            InfModelBuilderTestUtil.getReader(),
            InfModelBuilderTestUtil.getParser()
        );

        for (int i = 0; i < 100; i++)
            assertEquals(Integer.valueOf(i), infMdl.predict(i).get());
    }
}
