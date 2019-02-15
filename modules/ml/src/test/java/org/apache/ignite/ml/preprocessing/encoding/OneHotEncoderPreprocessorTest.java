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

package org.apache.ignite.ml.preprocessing.encoding;

import java.util.HashMap;
import java.util.HashSet;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownCategorialFeatureValue;
import org.apache.ignite.ml.preprocessing.encoding.onehotencoder.OneHotEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderPreprocessor;
import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link StringEncoderPreprocessor}.
 */
public class OneHotEncoderPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApplyWithStringValues() {
        String[][] data = new String[][]{
            {"1", "Moscow", "A"},
            {"2", "Moscow", "A"},
            {"2", "Moscow", "B"},
        };

        OneHotEncoderPreprocessor<Integer, String[]> preprocessor = new OneHotEncoderPreprocessor<Integer, String[]>(
            new HashMap[]{new HashMap() {
                {
                    put("1", 1);
                    put("2", 0);
                }
            }, new HashMap() {
                {
                    put("Moscow", 0);
                }
            }, new HashMap() {
                {
                    put("A", 0);
                    put("B", 1);
                }
            }},
            (k, v) -> v,
            new HashSet() {
                {
                    add(0);
                    add(1);
                    add(2);
                }
            });

        double[][] postProcessedData = new double[][]{
            {1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0},
            {0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0},
            {0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0},
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);
    }


    /**
     * The {@code apply()} method is failed with UnknownCategorialFeatureValue exception.
     *
     * The reason is missed information in encodingValues.
     *
     * @see UnknownCategorialFeatureValue
     */
    @Test
    public void testApplyWithUnknownGategorialValues() {
        String[][] data = new String[][]{
            {"1", "Moscow", "A"},
            {"2", "Moscow", "A"},
            {"2", "Moscow", "B"},
        };

        OneHotEncoderPreprocessor<Integer, String[]> preprocessor = new OneHotEncoderPreprocessor<Integer, String[]>(
            new HashMap[]{new HashMap() {
                {
                    put("2", 0);
                }
            }, new HashMap() {
                {
                    put("Moscow", 0);
                }
            }, new HashMap() {
                {
                    put("A", 0);
                    put("B", 1);
                }
            }},
            (k, v) -> v,
            new HashSet() {
                {
                    add(0);
                    add(1);
                    add(2);
                }
            });

        double[][] postProcessedData = new double[][]{
            {1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0},
            {0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0},
            {0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0},
        };

        try {
            for (int i = 0; i < data.length; i++)
                assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);

            fail("UnknownCategorialFeatureValue");
        } catch (UnknownCategorialFeatureValue e) {
            return;
        }
        fail("UnknownCategorialFeatureValue");
    }
}
