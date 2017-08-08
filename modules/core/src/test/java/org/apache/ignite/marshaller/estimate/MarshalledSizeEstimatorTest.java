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

package org.apache.ignite.marshaller.estimate;

import java.util.Date;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Common test for {@code MarshalledSizeEstimator} implementation(s).
 */
public class MarshalledSizeEstimatorTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSamplingMarshalledSizeEstimator() throws Exception {
        try {
            final IgniteEx ignite = startGrid(0);

            final Marshaller marshaller = ignite.configuration().getMarshaller();

            final MarshalledSizeEstimator estimator = new SamplingMarshalledSizeEstimator(new X100Sampler());

            final long estimatedSize = estimator.estimate(
                marshaller,
                new DataModel()
                    .className("org.apache.ignite.marshaller.estimate.MarshalledSizeEstimatorTest$ModelClassAlpha")
                    .count(10_000L)
                    .setFieldStats("dates", new DataModel.FieldStats()
                        .averageSize(100))
                    .setFieldStats("name", new DataModel.FieldStats()
                        .nullsPercent(20))
                ,
                new DataModel()
                    .className("org.apache.ignite.marshaller.estimate.MarshalledSizeEstimatorTest$ModelClassBeta")
                    .count(20_000L)
                    .setFieldStats("relatedIds", new DataModel.FieldStats()
                        .nullsPercent(25)
                        .averageSize(200))
                ,
                new DataModel()
                    .className("org.apache.ignite.marshaller.estimate.MarshalledSizeEstimatorTest$ModelClassGamma")
                    .count(30_000L)
                    .setFieldStats("data", new DataModel.FieldStats()
                        .nullsPercent(40))
                    .setFieldStats("data.intArray", new DataModel.FieldStats()
                        .nullsPercent(10)
                        .averageSize(200))
                    .setFieldStats("data.numbers", new DataModel.FieldStats()
                        .nullsPercent(35)
                        .averageSize(200))
                    .setFieldStats("dataArray", new DataModel.FieldStats()
                        .nullsPercent(5)
                        .averageSize(50))
                    .setFieldStats("dataArray.intArray", new DataModel.FieldStats()
                        .nullsPercent(10)
                        .averageSize(200))
                    .setFieldStats("dataArray.numbers", new DataModel.FieldStats()
                        .averageSize(20))
                    .setFieldStats("dataArray.amount", new DataModel.FieldStats()
                        .nullsPercent(5))
            );

            log.info("Estimated size: " + estimatedSize);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public static class ModelClassAlpha {
        private int count;
        private Date date;
        private Date[] dates;
        private String name = "ЙЦУКЕН ФЫВАПРОЛДЖ ЯЧСМИТЬБЮ";
        private Character character = new Character('\u1234');
    }

    /** */
    public static class ModelClassBeta {
        private long id;
        private Long[] relatedIds;
    }

    /** */
    public static class ModelClassGamma {
        private CustomData data;
        private CustomData[] dataArray;
    }

    /** */
    public static class CustomData {
        private int[] intArray;
        private Double amount;
        private Integer[] numbers;
    }
}
