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

package org.apache.ignite.yardstick.ml;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;

/**
 * Helps to ensure use of meaningfully different data in repeated invocations of benchmarked code in
 * {@link IgniteAbstractBenchmark#test(Map)} method.
 */
public class DataChanger {
    /** */
    private static final AtomicInteger changer = new AtomicInteger(0);

    /**
     * @return Positive "seed" to mutate data.
     */
    public static double next() {
        return (changer.incrementAndGet() % 17) + 1;
    }

    /** */
    public static class Scale {
        /** */
        private final double scale;

        /** */
        public Scale() {
            this.scale = DataChanger.next();
        }

        /** */
        public double[][] mutate(double[][] orig) {
            for (int i = 0; i < orig.length; i++)
                orig[i] = mutate(orig[i]);

            return orig;
        }

        /** */
        public double[] mutate(double[] orig) {
            for (int i = 0; i < orig.length; i++)
                orig[i] *= scale;

            return orig;
        }
    }
}
