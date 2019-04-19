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

import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.inference.reader.ModelReader;

/**
 * Util class for model builder tests.
 */
class ModelBuilderTestUtil {
    /**
     * Creates dummy model reader used in tests.
     *
     * @return Dummy model reader used in tests.
     */
    static ModelReader getReader() {
        return () -> new byte[0];
    }

    /**
     * Creates dummy model parser used in tests.
     *
     * @return Dummy model parser used in tests.
     */
    static ModelParser<Integer, Integer, Model<Integer, Integer>> getParser() {
        return m -> new Model<Integer, Integer>() {
            @Override public Integer predict(Integer input) {
                return input;
            }

            @Override public void close() {
                // Do nothing.
            }
        };
    }
}
