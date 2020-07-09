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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.GDBTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.svm.SVMLinearClassificationModel;
import org.apache.ignite.ml.tree.DecisionTreeConditionalNode;
import org.apache.ignite.ml.tree.DecisionTreeLeafNode;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Parser of Spark models. */
public class SparkModelParser {
    /**
     * Load model from parquet (presented as a directory).
     *
     * @param pathToMdl Path to directory with saved model.
     * @param parsedSparkMdl Parsed spark model.
     * @param learningEnvironment Learning environment.
     */
    public static Model parse(String pathToMdl, SupportedSparkModels parsedSparkMdl,
        LearningEnvironment learningEnvironment) throws IllegalArgumentException {
        return extractModel(pathToMdl, parsedSparkMdl, learningEnvironment);
    }

    /**
     * Load model from parquet (presented as a directory).
     *
     * @param pathToMdl Path to directory with saved model.
     * @param parsedSparkMdl Parsed spark model.
     */
    public static Model parse(String pathToMdl, SupportedSparkModels parsedSparkMdl) throws IllegalArgumentException {
        LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();
        LearningEnvironment environment = envBuilder.buildForTrainer();
        return extractModel(pathToMdl, parsedSparkMdl, environment);
    }

    /**
     * @param pathToMdl Path to model.
     * @param parsedSparkMdl Parsed spark model.
     * @param learningEnvironment Learning environment.
     */
    private static Model extractModel(String pathToMdl, SupportedSparkModels parsedSparkMdl,
        LearningEnvironment learningEnvironment) {
        File mdlDir = IgniteUtils.resolveIgnitePath(pathToMdl);

        if (mdlDir == null) {
            String msg = "Directory not found or empty [directory_path=" + pathToMdl + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        if (!mdlDir.isDirectory()) {
            String msg = "Spark Model Parser supports loading from directory only. " +
                "The specified path " + pathToMdl + " is not the path to directory.";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        String[] files = mdlDir.list();
        if (files.length == 0) {
            String msg = "Directory contain 0 files and sub-directories [directory_path=" + pathToMdl + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        if (Arrays.stream(files).noneMatch("data"::equals)) {
            String msg = "Directory should contain data sub-directory [directory_path=" + pathToMdl + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        if (Arrays.stream(files).noneMatch("metadata"::equals)) {
            String msg = "Directory should contain metadata sub-directory [directory_path=" + pathToMdl + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        String pathToData = pathToMdl + File.separator + "data";
        File dataDir = IgniteUtils.resolveIgnitePath(pathToData);

        File[] dataParquetFiles = dataDir.listFiles((dir, name) -> name.matches("^part-.*\\.snappy\\.parquet$"));
        if (dataParquetFiles.length == 0) {
            String msg = "Directory should contain parquet file " +
                "with model [directory_path=" + pathToData + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        if (dataParquetFiles.length > 1) {
            String msg = "Directory should contain only one parquet file " +
                "with model [directory_path=" + pathToData + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        String pathToMdlFile = dataParquetFiles[0].getPath();

        String pathToMetadata = pathToMdl + File.separator + "metadata";
        File metadataDir = IgniteUtils.resolveIgnitePath(pathToMetadata);
        String[] metadataFiles = metadataDir.list();

        if (Arrays.stream(metadataFiles).noneMatch("part-00000"::equals)) {
            String msg = "Directory should contain json file with model metadata " +
                "with name part-00000 [directory_path=" + pathToMetadata + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        try {
            validateMetadata(pathToMetadata, parsedSparkMdl, learningEnvironment);
        }
        catch (FileNotFoundException e) {
            String msg = "Directory should contain json file with model metadata " +
                "with name part-00000 [directory_path=" + pathToMetadata + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        if (shouldContainTreeMetadataSubDirectory(parsedSparkMdl)) {
            if (Arrays.stream(files).noneMatch("treesMetadata"::equals)) {
                String msg = "Directory should contain treeMetadata sub-directory [directory_path=" + pathToMdl + "]";
                learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                throw new IllegalArgumentException(msg);
            }

            String pathToTreesMetadata = pathToMdl + File.separator + "treesMetadata";
            File treesMetadataDir = IgniteUtils.resolveIgnitePath(pathToTreesMetadata);

            File[] treesMetadataParquetFiles = treesMetadataDir.listFiles((dir, name) -> name.matches("^part-.*\\.snappy\\.parquet$"));
            if (treesMetadataParquetFiles.length == 0) {
                String msg = "Directory should contain parquet file " +
                    "with model treesMetadata [directory_path=" + pathToTreesMetadata + "]";
                learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                throw new IllegalArgumentException(msg);
            }

            if (treesMetadataParquetFiles.length > 1) {
                String msg = "Directory should contain only one parquet file " +
                    "with model [directory_path=" + pathToTreesMetadata + "]";
                learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                throw new IllegalArgumentException(msg);
            }

            String pathToTreesMetadataFile = treesMetadataParquetFiles[0].getPath();

            return parseDataWithMetadata(pathToMdlFile, pathToTreesMetadataFile, parsedSparkMdl, learningEnvironment);
        }
        else
            return parseData(pathToMdlFile, parsedSparkMdl, learningEnvironment);
    }

    /**
     * Validate metadata json file.
     *
     * NOTE: file is exists due to previous validation step.
     *
     * @param pathToMetadata Path to metadata.
     * @param parsedSparkMdl Parsed spark model.
     * @param learningEnvironment Learning environment.
     */
    private static void validateMetadata(String pathToMetadata,
        SupportedSparkModels parsedSparkMdl, LearningEnvironment learningEnvironment) throws FileNotFoundException {
        File metadataFile = IgniteUtils.resolveIgnitePath(pathToMetadata + File.separator + "part-00000");
        if (metadataFile != null) {
            Scanner sc = new Scanner(metadataFile);
            boolean isInvalid = true;
            while (sc.hasNextLine()) {
                final String line = sc.nextLine();
                if (line.contains(parsedSparkMdl.getMdlClsNameInSpark()))
                    isInvalid = false;
            }

            if (isInvalid) {
                String msg = "The metadata file contains incorrect model metadata. " +
                    "It should contain " + parsedSparkMdl.getMdlClsNameInSpark() + " model metadata.";
                learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }

    /**
     * @param parsedSparkMdl Parsed spark model.
     */
    private static boolean shouldContainTreeMetadataSubDirectory(SupportedSparkModels parsedSparkMdl) {
        return parsedSparkMdl == SupportedSparkModels.GRADIENT_BOOSTED_TREES
            || parsedSparkMdl == SupportedSparkModels.GRADIENT_BOOSTED_TREES_REGRESSION;
    }

    /**
     * Load model from parquet file.
     *
     * @param pathToMdl Hadoop path to model saved from Spark.
     * @param parsedSparkMdl One of supported Spark models to parse it.
     * @param learningEnvironment Learning environment.
     * @return Instance of parsedSparkMdl model.
     */
    private static Model parseData(String pathToMdl, SupportedSparkModels parsedSparkMdl,
        LearningEnvironment learningEnvironment) {
        File mdlRsrc = IgniteUtils.resolveIgnitePath(pathToMdl);
        if (mdlRsrc == null) {
            String msg = "Resource not found [resource_path=" + pathToMdl + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        String ignitePathToMdl = mdlRsrc.getPath();
        learningEnvironment.logger().log(MLLogger.VerboseLevel.LOW, "Starting loading model by the path: " + ignitePathToMdl);

        switch (parsedSparkMdl) {
            case LOG_REGRESSION:
                return loadLogRegModel(ignitePathToMdl, learningEnvironment);
            case LINEAR_REGRESSION:
                return loadLinRegModel(ignitePathToMdl, learningEnvironment);
            case LINEAR_SVM:
                return loadLinearSVMModel(ignitePathToMdl, learningEnvironment);
            case DECISION_TREE:
                return loadDecisionTreeModel(ignitePathToMdl, learningEnvironment);
            case RANDOM_FOREST:
                return loadRandomForestModel(ignitePathToMdl, learningEnvironment);
            case KMEANS:
                return loadKMeansModel(ignitePathToMdl, learningEnvironment);
            case DECISION_TREE_REGRESSION:
                return loadDecisionTreeRegressionModel(ignitePathToMdl, learningEnvironment);
            case RANDOM_FOREST_REGRESSION:
                return loadRandomForestRegressionModel(ignitePathToMdl, learningEnvironment);
            default:
                throw new UnsupportedSparkModelException(ignitePathToMdl);
        }
    }

    /**
     * Load model and its metadata from parquet files.
     *
     * @param pathToMdl Hadoop path to model saved from Spark.
     * @param pathToMetaData Hadoop path to metadata saved from Spark.
     * @param parsedSparkMdl One of supported Spark models to parse it.
     * @param learningEnvironment Learning environment.
     * @return Instance of parsedSparkMdl model.
     */
    private static Model parseDataWithMetadata(String pathToMdl, String pathToMetaData,
        SupportedSparkModels parsedSparkMdl, LearningEnvironment learningEnvironment) {
        File mdlRsrc1 = IgniteUtils.resolveIgnitePath(pathToMdl);
        if (mdlRsrc1 == null) {
            String msg = "Resource not found [resource_path=" + pathToMdl + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        String ignitePathToMdl = mdlRsrc1.getPath();
        learningEnvironment.logger().log(MLLogger.VerboseLevel.LOW, "Starting loading model by the path: " + ignitePathToMdl);

        File mdlRsrc2 = IgniteUtils.resolveIgnitePath(pathToMetaData);
        if (mdlRsrc2 == null) {
            String msg = "Resource not found [resource_path=" + pathToMetaData + "]";
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new IllegalArgumentException(msg);
        }

        String ignitePathToMdlMetaData = mdlRsrc2.getPath();
        learningEnvironment.logger().log(MLLogger.VerboseLevel.LOW, "Starting loading model metadata by the path: " + ignitePathToMdlMetaData);

        switch (parsedSparkMdl) {
            case GRADIENT_BOOSTED_TREES:
                return loadGBTClassifierModel(ignitePathToMdl, ignitePathToMdlMetaData, learningEnvironment);
            case GRADIENT_BOOSTED_TREES_REGRESSION:
                return loadGBTRegressionModel(ignitePathToMdl, ignitePathToMdlMetaData, learningEnvironment);
            default:
                throw new UnsupportedSparkModelException(ignitePathToMdl);
        }
    }

    /**
     * Load Random Forest Regression model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static Model loadRandomForestRegressionModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        final List<IgniteModel<Vector, Double>> models = parseTreesForRandomForestAlgorithm(pathToMdl, learningEnvironment);
        if (models == null)
            return null;
        return new ModelsComposition(models, new MeanValuePredictionsAggregator());
    }

    /**
     * Load Decision Tree Regression model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment learningEnvironment
     */
    private static Model loadDecisionTreeRegressionModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        return loadDecisionTreeModel(pathToMdl, learningEnvironment);
    }

    /**
     * Load K-Means model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment learningEnvironment
     */
    private static Model loadKMeansModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        Vector[] centers = null;

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;
            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pages = r.readNextRowGroup())) {
                final int rows = (int)pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                centers = new DenseVector[rows];

                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    // final int clusterIdx = g.getInteger(0, 0);

                    Group clusterCenterCoeff = g.getGroup(1, 0).getGroup(3, 0);

                    final int amountOfCoefficients = clusterCenterCoeff.getFieldRepetitionCount(0);

                    centers[i] = new DenseVector(amountOfCoefficients);

                    for (int j = 0; j < amountOfCoefficients; j++) {
                        double coefficient = clusterCenterCoeff.getGroup(0, j).getDouble(0, 0);
                        centers[i].set(j, coefficient);
                    }
                }
            }

        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }

        return new KMeansModel(centers, new EuclideanDistance());
    }

    /**
     * Load GDB Regression model.
     *
     * @param pathToMdl Path to model.
     * @param pathToMdlMetaData Path to model meta data.
     * @param learningEnvironment learningEnvironment
     */
    private static Model loadGBTRegressionModel(String pathToMdl, String pathToMdlMetaData,
        LearningEnvironment learningEnvironment) {
        IgniteFunction<Double, Double> lbMapper = lb -> lb;

        return parseAndBuildGDBModel(pathToMdl, pathToMdlMetaData, lbMapper, learningEnvironment);
    }

    /**
     * Load GDB Classification model.
     *
     * @param pathToMdl Path to model.
     * @param pathToMdlMetaData Path to model meta data.
     * @param learningEnvironment learningEnvironment
     */
    private static Model loadGBTClassifierModel(String pathToMdl, String pathToMdlMetaData,
        LearningEnvironment learningEnvironment) {
        IgniteFunction<Double, Double> lbMapper = lb -> lb > 0.5 ? 1.0 : 0.0;

        return parseAndBuildGDBModel(pathToMdl, pathToMdlMetaData, lbMapper, learningEnvironment);
    }

    /**
     * Parse and build common GDB model with the custom label mapper.
     *
     * @param pathToMdl Path to model.
     * @param pathToMdlMetaData Path to model meta data.
     * @param lbMapper Label mapper.
     * @param learningEnvironment learningEnvironment
     */
    @Nullable private static Model parseAndBuildGDBModel(String pathToMdl, String pathToMdlMetaData,
        IgniteFunction<Double, Double> lbMapper, LearningEnvironment learningEnvironment) {
        double[] treeWeights = null;
        final Map<Integer, Double> treeWeightsByTreeID = new HashMap<>();

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdlMetaData), new Configuration()))) {
            PageReadStore pagesMetaData;
            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pagesMetaData = r.readNextRowGroup())) {
                final long rows = pagesMetaData.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pagesMetaData, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    int treeId = g.getInteger(0, 0);
                    double treeWeight = g.getDouble(2, 0);
                    treeWeightsByTreeID.put(treeId, treeWeight);
                }
            }
        }
        catch (IOException e) {
            String msg = "Error reading parquet file with MetaData by the path: " + pathToMdlMetaData;
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }

        treeWeights = new double[treeWeightsByTreeID.size()];
        for (int i = 0; i < treeWeights.length; i++)
            treeWeights[i] = treeWeightsByTreeID.get(i);

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;
            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);
            final Map<Integer, TreeMap<Integer, NodeData>> nodesByTreeId = new TreeMap<>();
            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    final int treeID = g.getInteger(0, 0);

                    final SimpleGroup nodeDataGroup = (SimpleGroup)g.getGroup(1, 0);
                    NodeData nodeData = extractNodeDataFromParquetRow(nodeDataGroup);

                    if (nodesByTreeId.containsKey(treeID)) {
                        Map<Integer, NodeData> nodesByNodeId = nodesByTreeId.get(treeID);
                        nodesByNodeId.put(nodeData.id, nodeData);
                    }
                    else {
                        TreeMap<Integer, NodeData> nodesByNodeId = new TreeMap<>();
                        nodesByNodeId.put(nodeData.id, nodeData);
                        nodesByTreeId.put(treeID, nodesByNodeId);
                    }
                }
            }

            final List<IgniteModel<Vector, Double>> models = new ArrayList<>();
            nodesByTreeId.forEach((key, nodes) -> models.add(buildDecisionTreeModel(nodes)));

            return new GDBTrainer.GDBModel(models, new WeightedPredictionsAggregator(treeWeights), lbMapper);
        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Load RF model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static Model loadRandomForestModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        final List<IgniteModel<Vector, Double>> models = parseTreesForRandomForestAlgorithm(pathToMdl, learningEnvironment);
        if (models == null)
            return null;
        return new ModelsComposition(models, new OnMajorityPredictionsAggregator());
    }

    /**
     * Parse trees from file for common Random Forest ensemble.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static List<IgniteModel<Vector, Double>> parseTreesForRandomForestAlgorithm(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;

            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);
            final Map<Integer, TreeMap<Integer, NodeData>> nodesByTreeId = new TreeMap<>();

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    final int treeID = g.getInteger(0, 0);
                    final SimpleGroup nodeDataGroup = (SimpleGroup)g.getGroup(1, 0);

                    NodeData nodeData = extractNodeDataFromParquetRow(nodeDataGroup);

                    if (nodesByTreeId.containsKey(treeID)) {
                        Map<Integer, NodeData> nodesByNodeId = nodesByTreeId.get(treeID);
                        nodesByNodeId.put(nodeData.id, nodeData);
                    }
                    else {
                        TreeMap<Integer, NodeData> nodesByNodeId = new TreeMap<>();
                        nodesByNodeId.put(nodeData.id, nodeData);
                        nodesByTreeId.put(treeID, nodesByNodeId);
                    }
                }
            }
            List<IgniteModel<Vector, Double>> models = new ArrayList<>();
            nodesByTreeId.forEach((key, nodes) -> models.add(buildDecisionTreeModel(nodes)));
            return models;
        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Load Decision Tree model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static Model loadDecisionTreeModel(String pathToMdl, LearningEnvironment learningEnvironment) {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;

            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);
            final Map<Integer, NodeData> nodes = new TreeMap<>();

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    NodeData nodeData = extractNodeDataFromParquetRow(g);
                    nodes.put(nodeData.id, nodeData);
                }
            }
            return buildDecisionTreeModel(nodes);
        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Builds the DT model by the given sorted map of nodes.
     *
     * @param nodes The sorted map of nodes.
     */
    private static DecisionTreeNode buildDecisionTreeModel(Map<Integer, NodeData> nodes) {
        DecisionTreeNode mdl = null;
        if (!nodes.isEmpty()) {
            NodeData rootNodeData = (NodeData)((NavigableMap)nodes).firstEntry().getValue();
            mdl = buildTree(nodes, rootNodeData);
            return mdl;
        }
        return mdl;
    }

    /**
     * Build tree or sub-tree based on indices and nodes sorted map as a dictionary.
     *
     * @param nodes The sorted map of nodes.
     * @param rootNodeData Root node data.
     */
    @NotNull private static DecisionTreeNode buildTree(Map<Integer, NodeData> nodes,
        NodeData rootNodeData) {
        return rootNodeData.isLeafNode ? new DecisionTreeLeafNode(rootNodeData.prediction) : new DecisionTreeConditionalNode(rootNodeData.featureIdx,
            rootNodeData.threshold,
            buildTree(nodes, nodes.get(rootNodeData.rightChildId)),
            buildTree(nodes, nodes.get(rootNodeData.leftChildId)),
            null);
    }

    /**
     * Form the node data according data in parquet row.
     *
     * @param g The given group presenting the node data from Spark DT model.
     */
    @NotNull private static SparkModelParser.NodeData extractNodeDataFromParquetRow(SimpleGroup g) {
        NodeData nodeData = new NodeData();

        nodeData.id = g.getInteger(0, 0);
        nodeData.prediction = g.getDouble(1, 0);
        nodeData.leftChildId = g.getInteger(5, 0);
        nodeData.rightChildId = g.getInteger(6, 0);

        if (nodeData.leftChildId == -1 && nodeData.rightChildId == -1) {
            nodeData.featureIdx = -1;
            nodeData.threshold = -1;
            nodeData.isLeafNode = true;
        }
        else {
            final SimpleGroup splitGrp = (SimpleGroup)g.getGroup(7, 0);
            nodeData.featureIdx = splitGrp.getInteger(0, 0);
            nodeData.threshold = splitGrp.getGroup(1, 0).getGroup(0, 0).getDouble(0, 0);
        }
        return nodeData;
    }

    /**
     * Prints the given group in the row of Parquet file.
     *
     * @param g The given group.
     */
    private static void printGroup(Group g) {
        int fieldCnt = g.getType().getFieldCount();
        for (int field = 0; field < fieldCnt; field++) {
            int valCnt = g.getFieldRepetitionCount(field);

            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();

            for (int idx = 0; idx < valCnt; idx++) {
                if (fieldType.isPrimitive())
                    System.out.println(fieldName + " " + g.getValueToString(field, idx));
                else
                    printGroup(g.getGroup(field, idx));
            }
        }
        System.out.println();
    }

    /**
     * Load SVM model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static Model loadLinearSVMModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        Vector coefficients = null;
        double interceptor = 0;

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;

            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    interceptor = readSVMInterceptor(g);
                    coefficients = readSVMCoefficients(g);
                }
            }
        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }

        return new SVMLinearClassificationModel(coefficients, interceptor);
    }

    /**
     * Load linear regression model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static Model loadLinRegModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        Vector coefficients = null;
        double interceptor = 0;

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;

            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    interceptor = readLinRegInterceptor(g);
                    coefficients = readLinRegCoefficients(g);
                }
            }

        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }

        return new LinearRegressionModel(coefficients, interceptor);
    }

    /**
     * Load logistic regression model.
     *
     * @param pathToMdl Path to model.
     * @param learningEnvironment Learning environment.
     */
    private static Model loadLogRegModel(String pathToMdl,
        LearningEnvironment learningEnvironment) {
        Vector coefficients = null;
        double interceptor = 0;

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;

            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    interceptor = readInterceptor(g);
                    coefficients = readCoefficients(g);
                }
            }

        }
        catch (IOException e) {
            String msg = "Error reading parquet file: " + e.getMessage();
            learningEnvironment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            e.printStackTrace();
        }

        return new LogisticRegressionModel(coefficients, interceptor);
    }

    /**
     * Read interceptor value from parquet.
     *
     * @param g Interceptor group.
     */
    private static double readSVMInterceptor(SimpleGroup g) {
        return g.getDouble(1, 0);
    }

    /**
     * Read coefficient matrix from parquet.
     *
     * @param g Coefficient group.
     * @return Vector of coefficients.
     */
    private static Vector readSVMCoefficients(SimpleGroup g) {
        Vector coefficients;
        Group coeffGroup = g.getGroup(0, 0).getGroup(3, 0);

        final int amountOfCoefficients = coeffGroup.getFieldRepetitionCount(0);

        coefficients = new DenseVector(amountOfCoefficients);

        for (int j = 0; j < amountOfCoefficients; j++) {
            double coefficient = coeffGroup.getGroup(0, j).getDouble(0, 0);
            coefficients.set(j, coefficient);
        }
        return coefficients;
    }

    /**
     * Read interceptor value from parquet.
     *
     * @param g Interceptor group.
     */
    private static double readLinRegInterceptor(SimpleGroup g) {
        return g.getDouble(0, 0);
    }

    /**
     * Read coefficient matrix from parquet.
     *
     * @param g Coefficient group.
     * @return Vector of coefficients.
     */
    private static Vector readLinRegCoefficients(SimpleGroup g) {
        Vector coefficients;
        Group coeffGroup = g.getGroup(1, 0).getGroup(3, 0);

        final int amountOfCoefficients = coeffGroup.getFieldRepetitionCount(0);

        coefficients = new DenseVector(amountOfCoefficients);

        for (int j = 0; j < amountOfCoefficients; j++) {
            double coefficient = coeffGroup.getGroup(0, j).getDouble(0, 0);
            coefficients.set(j, coefficient);
        }
        return coefficients;
    }

    /**
     * Read interceptor value from parquet.
     *
     * @param g Interceptor group.
     */
    private static double readInterceptor(SimpleGroup g) {
        double interceptor;

        final SimpleGroup interceptVector = (SimpleGroup)g.getGroup(2, 0);
        final SimpleGroup interceptVectorVal = (SimpleGroup)interceptVector.getGroup(3, 0);
        final SimpleGroup interceptVectorValElement = (SimpleGroup)interceptVectorVal.getGroup(0, 0);

        interceptor = interceptVectorValElement.getDouble(0, 0);

        return interceptor;
    }

    /**
     * Read coefficient matrix from parquet.
     *
     * @param g Coefficient group.
     * @return Vector of coefficients.
     */
    private static Vector readCoefficients(SimpleGroup g) {
        Vector coefficients;
        final int amountOfCoefficients = g.getGroup(3, 0).getGroup(5, 0).getFieldRepetitionCount(0);

        coefficients = new DenseVector(amountOfCoefficients);

        for (int j = 0; j < amountOfCoefficients; j++) {
            double coefficient = g.getGroup(3, 0).getGroup(5, 0).getGroup(0, j).getDouble(0, 0);
            coefficients.set(j, coefficient);
        }
        return coefficients;
    }

    /**
     * Presenting data from one parquet row filled with NodeData in Spark DT model.
     */
    private static class NodeData {
        /** Id. */
        int id;

        /** Prediction. */
        double prediction;

        /** Left child id. */
        int leftChildId;

        /** Right child id. */
        int rightChildId;

        /** Threshold. */
        double threshold;

        /** Feature index. */
        int featureIdx;

        /** Is leaf node. */
        boolean isLeafNode;

        /** {@inheritDoc} */
        @Override public String toString() {
            return "NodeData{" +
                "id=" + id +
                ", prediction=" + prediction +
                ", leftChildId=" + leftChildId +
                ", rightChildId=" + rightChildId +
                ", threshold=" + threshold +
                ", featureIdx=" + featureIdx +
                ", isLeafNode=" + isLeafNode +
                '}';
        }
    }
}
