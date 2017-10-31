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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helps mutating data between benchmark invocations.
 * IMPL NOTE introduced per review comments of IGNITE-6123.
 */
class DataChanger {
    /** */
    private static final AtomicInteger changer = new AtomicInteger(0);

    /**
     * @return Positive "seed" to mutate data.
     */
    static double next() {
        return (changer.incrementAndGet() % 17) + 1;
    }

    /** */
    static class Scale {
        /** */
        private final double scale;

        /** */
        Scale() {
            this.scale = DataChanger.next();
        }

        /** */
        double[][] mutate(double[][] orig) {
            for (int i = 0; i < orig.length; i++)
                orig[i] = mutate(orig[i]);

            return orig;
        }

        /** */
        double[] mutate(double[] orig) {
            for (int i = 0; i < orig.length; i++)
                orig[i] *= scale;

            return orig;
        }
    }
}
