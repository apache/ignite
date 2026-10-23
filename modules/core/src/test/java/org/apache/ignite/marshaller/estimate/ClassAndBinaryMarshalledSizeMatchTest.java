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
 *
 */
public class ClassAndBinaryMarshalledSizeMatchTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testEstimatedSizesMatch() throws Exception {
        try {
            final IgniteEx ignite = startGrid(0);

            final Marshaller marshaller = ignite.configuration().getMarshaller();

            final MarshalledSizeEstimator estimator = new SamplingMarshalledSizeEstimator(
                new X100Sampler(new SampleFactoryImpl(ignite.binary())));

            final EstimationDataModel dataModel1 = new EstimationDataModel()
                .className("org.apache.ignite.marshaller.estimate.ClassAndBinaryMarshalledSizeMatchTest$Model1")
                .count(10_000L);

            final long estimatedSize1a = estimator.estimate(marshaller, dataModel1);

            log.info("Estimated size #1a: " + estimatedSize1a);

            final long estimatedSize1b = estimator.estimate(
                marshaller,
                dataModel1
                    .setFieldType("count", "int")
            );

            log.info("Estimated size #1b: " + estimatedSize1b);

            if (estimatedSize1a != estimatedSize1b)
                log.warning("Estimated sizes #1a and #1b are different, delta: " + (estimatedSize1b - estimatedSize1a));

            final EstimationDataModel dataModel2 = new EstimationDataModel()
                .className("org.apache.ignite.marshaller.estimate.ClassAndBinaryMarshalledSizeMatchTest$Model2")
                .count(10_000L);

            final long estimatedSize2a = estimator.estimate(marshaller, dataModel2);

            log.info("Estimated size #2a: " + estimatedSize2a);

            final long estimatedSize2b = estimator.estimate(
                marshaller,
                dataModel2
                    .setFieldType("count", "int")
                    .setFieldType("date", Date.class.getName())
            );

            log.info("Estimated size #2b: " + estimatedSize2b);

            if (estimatedSize2a != estimatedSize2b)
                log.warning("Estimated sizes #2a and #2b are different, delta: " + (estimatedSize2b - estimatedSize2a));

            final EstimationDataModel dataModel3 = new EstimationDataModel()
                .className("org.apache.ignite.marshaller.estimate.ClassAndBinaryMarshalledSizeMatchTest$Model3")
                .count(10_000L);

            final long estimatedSize3a = estimator.estimate(marshaller, dataModel3);

            log.info("Estimated size #3a: " + estimatedSize3a);

            final long estimatedSize3b = estimator.estimate(
                marshaller,
                dataModel3
                    .setFieldType("count", "int")
                    .setFieldType("character", "char")
            );

            log.info("Estimated size #3b: " + estimatedSize3b);

            if (estimatedSize3a != estimatedSize3b)
                log.warning("Estimated sizes #3a and #3b are different, delta: " + (estimatedSize3b - estimatedSize3a));

            final EstimationDataModel dataModel4 = new EstimationDataModel()
                .className("org.apache.ignite.marshaller.estimate.ClassAndBinaryMarshalledSizeMatchTest$Model4")
                .count(10_000L);

            final long estimatedSize4a = estimator.estimate(marshaller, dataModel4);

            log.info("Estimated size #4a: " + estimatedSize4a);

            final long estimatedSize4b = estimator.estimate(
                marshaller,
                dataModel4
                    .setFieldType("character", "char")
            );

            log.info("Estimated size #4b: " + estimatedSize4b);

            if (estimatedSize4a != estimatedSize4b)
                log.warning("Estimated sizes #4a and #4b are different, delta: " + (estimatedSize4b - estimatedSize4a));

            final EstimationDataModel dataModel5 = new EstimationDataModel()
                .className("org.apache.ignite.marshaller.estimate.ClassAndBinaryMarshalledSizeMatchTest$Model5")
                .count(10_000L)
                .setFieldStats("dates", new EstimationDataModel.FieldStats()
                    .averageSize(100))
                .setFieldStats("name", new EstimationDataModel.FieldStats()
                    .nullsPercent(20)
                    .averageSize(27));

            final long estimatedSize5a = estimator.estimate(marshaller, dataModel5);

            log.info("Estimated size #5a: " + estimatedSize5a);

            final long estimatedSize5b = estimator.estimate(
                marshaller,
                dataModel5
                    .setFieldType("date", Date.class.getName())
                    .setFieldType("dates", Date[].class.getName())
                    .setFieldType("name", String.class.getName())
            );

            log.info("Estimated size #5b: " + estimatedSize5b);

            if (estimatedSize5a != estimatedSize5b)
                log.warning("Estimated sizes #5a and #5b are different, delta: " + (estimatedSize5b - estimatedSize5a));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public static class Model1 {
        private int count;
    }

    /** */
    public static class Model2 {
        private int count;
        private Date date;
    }

    /** */
    public static class Model3 {
        private int count;
        private char character;
    }

    /** */
    public static class Model4 {
        private char character;
    }

    /** */
    public static class Model5 {
        private Date date;
        private Date[] dates;
        private String name;
    }
}
