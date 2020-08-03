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

package org.apache.ignite.ml.sparkmodelparser;

import java.io.File;
import java.net.URL;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SparkModelParser}.
 */
public class SparkModelParserTest {
    /** Learning environment. */
    LearningEnvironment env = LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyTypeDependency(ParallelismStrategy.ON_DEFAULT_POOL)
        .withLoggingFactoryDependency(ConsoleLogger.Factory.HIGH).buildForTrainer();

    /** Expected exception. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /** Path to empty directory. */
    public static final String SPARK_MDL_PATH = "models";

    /**
     * Fails on null directory.
     */
    @Test
    public void failOnNullDirectory() {
        try {
            SparkModelParser.parse(
                "incorrectPath", SupportedSparkModels.LINEAR_REGRESSION
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory not found or empty"));
        }
    }

    /**
     * Fails on empty directory.
     */
    @Test
    public void failOnEmptyDirectory() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + "empty", SupportedSparkModels.LINEAR_REGRESSION, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory not found or empty"));
        }
    }

    /**
     * Fails on empty directory with empty subfolder.
     */
    @Test
    public void failOnEmptyOrNonExistingDataDirectory() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "nodatafolder", SupportedSparkModels.LINEAR_REGRESSION, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain data sub-directory"));
        }
    }

    /**
     * Fails on empty directory with empty subfolder.
     */
    @Test
    public void failOnEmptyOrNonExistingMetadataDirectory() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "nometadatafolder", SupportedSparkModels.LINEAR_REGRESSION, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain metadata sub-directory"));
        }
    }

    /**
     * Fails on non-existing model parquet file.
     */
    @Test
    public void failOnNonExistingModelParquetFile() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "nomodelfilefolder", SupportedSparkModels.LINEAR_REGRESSION, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain parquet file"));
        }
    }

    /**
     * Fails on two existing model parquet file.
     */
    @Test
    public void failOnTwoExistingModelParquetFile() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "twomodelfilefolder", SupportedSparkModels.LINEAR_REGRESSION, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain only one parquet file"));
        }
    }

    /**
     * Fails on two existing model parquet file.
     */
    @Test
    public void failOnNonExistingMetadataFile() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "nometadatafilefolder", SupportedSparkModels.LINEAR_REGRESSION, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain json file with model metadata"));
        }
    }

    /**
     * Fails on non-existing treesMetadataFolder for GBT models.
     */
    @Test
    public void failOnNonExistingTreeMetadataFolder() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "notreesmetadatafolder", SupportedSparkModels.GRADIENT_BOOSTED_TREES, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain treeMetadata sub-directory"));
        }
    }

    /**
     * Fails on non-existing treesMetadata parquet file for GBT models.
     */
    @Test
    public void failOnNonExistingTreeMetadataFile() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "notreesmetadatafile", SupportedSparkModels.GRADIENT_BOOSTED_TREES, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain parquet file"));
        }
    }

    /**
     * Fails on two existing treesMetadata parquet file for GBT models.
     */
    @Test
    public void failOnTwoExistingTreeMetadataFile() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "twotreesmetadatafiles", SupportedSparkModels.GRADIENT_BOOSTED_TREES, env
            );
            fail("Expected IllegalArgumentException exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Directory should contain only one parquet file"));
        }
    }

    /**
     * Fails on incorrect model class loading.
     *
     * NOTE: Trying to load Decision Tree model from GBT directory.
     */
    @Test
    public void failOnIncorrectModelClassLoading() {
        URL url = getClass().getClassLoader().getResource(SPARK_MDL_PATH);

        try {
            SparkModelParser.parse(
                url.getPath() + File.separator + "gbt", SupportedSparkModels.DECISION_TREE, env
            );
            fail("Expected IllegalArgumentException exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("The metadata file contains incorrect model metadata."));
        }
    }
}
