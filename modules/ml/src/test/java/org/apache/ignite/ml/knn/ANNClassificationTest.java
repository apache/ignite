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

package org.apache.ignite.ml.knn;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.knn.ann.ANNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationTrainer;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests behaviour of ANNClassificationTest. */
public class ANNClassificationTest extends TrainerTest {
    /** */
    @Test
    public void testBinaryClassification() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoClusters.length; i++)
            cacheMock.put(i, twoClusters[i]);

        ANNClassificationTrainer trainer = new ANNClassificationTrainer()
            .withK(10)
            .withMaxIterations(10)
            .withEpsilon(1e-4)
            .withDistance(new EuclideanDistance());

        Assert.assertEquals(10, trainer.getK());
        Assert.assertEquals(10, trainer.getMaxIterations());
        TestUtils.assertEquals(1e-4, trainer.getEpsilon(), PRECISION);
        Assert.assertEquals(new EuclideanDistance(), trainer.getDistance());

        NNClassificationModel mdl = trainer.fit(
            cacheMock,
            parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withWeighted(false);

        Assert.assertNotNull(((ANNClassificationModel) mdl).getCandidates());

        assertTrue(mdl.toString().contains("weighted = [false]"));
        assertTrue(mdl.toString(true).contains("weighted = [false]"));
        assertTrue(mdl.toString(false).contains("weighted = [false]"));
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoClusters.length; i++)
            cacheMock.put(i, twoClusters[i]);

        ANNClassificationTrainer trainer = new ANNClassificationTrainer()
            .withK(10)
            .withMaxIterations(10)
            .withEpsilon(1e-4)
            .withDistance(new EuclideanDistance());

        ANNClassificationModel originalMdl = (ANNClassificationModel) trainer.fit(
            cacheMock,
            parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withWeighted(false);

        ANNClassificationModel updatedOnSameDataset = (ANNClassificationModel) trainer.update(originalMdl,
            cacheMock, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withWeighted(false);

        ANNClassificationModel updatedOnEmptyDataset = (ANNClassificationModel) trainer.update(originalMdl,
            new HashMap<>(), parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withWeighted(false);

        Assert.assertNotNull(updatedOnSameDataset.getCandidates());

        assertTrue(updatedOnSameDataset.toString().contains("weighted = [false]"));
        assertTrue(updatedOnSameDataset.toString(true).contains("weighted = [false]"));
        assertTrue(updatedOnSameDataset.toString(false).contains("weighted = [false]"));

        assertNotNull(updatedOnEmptyDataset.getCandidates());

        assertTrue(updatedOnEmptyDataset.toString().contains("weighted = [false]"));
        assertTrue(updatedOnEmptyDataset.toString(true).contains("weighted = [false]"));
        assertTrue(updatedOnEmptyDataset.toString(false).contains("weighted = [false]"));
    }
}
