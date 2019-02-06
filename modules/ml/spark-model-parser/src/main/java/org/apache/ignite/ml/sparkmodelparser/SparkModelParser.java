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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
     * Load model from parquet file.
     *
     * @param pathToMdl Hadoop path to model saved from Spark.
     * @param parsedSparkMdl One of supported Spark models to parse it.
     * @return Instance of parsedSparkMdl model.
     */
    public static Model parse(String pathToMdl, SupportedSparkModels parsedSparkMdl) {
        File mdlRsrc = IgniteUtils.resolveIgnitePath(pathToMdl);
        if (mdlRsrc == null)
            throw new IllegalArgumentException("Resource not found [resource_path=" + pathToMdl + "]");

        String ignitePathToMdl = mdlRsrc.getPath();

        switch (parsedSparkMdl) {
            case LOG_REGRESSION:
                return loadLogRegModel(ignitePathToMdl);
            case LINEAR_REGRESSION:
                return loadLinRegModel(ignitePathToMdl);
            case LINEAR_SVM:
                return loadLinearSVMModel(ignitePathToMdl);
            case DECISION_TREE:
                return loadDecisionTreeModel(ignitePathToMdl);
            case RANDOM_FOREST:
                return loadRandomForestModel(ignitePathToMdl);
            case KMEANS:
                return loadKMeansModel(ignitePathToMdl);
            case DECISION_TREE_REGRESSION:
                return loadDecisionTreeRegressionModel(ignitePathToMdl);
            case RANDOM_FOREST_REGRESSION:
                return loadRandomForestRegressionModel(ignitePathToMdl);
            default:
                throw new UnsupportedSparkModelException(ignitePathToMdl);
        }
    }

    /**
     * Load Random Forest Regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadRandomForestRegressionModel(String pathToMdl) {
        final List<IgniteModel<Vector, Double>> models = parseTreesForRandomForestAlgorithm(pathToMdl);
        if (models == null)
            return null;
        return new ModelsComposition(models, new MeanValuePredictionsAggregator());
    }

    /**
     * Load Decision Tree Regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadDecisionTreeRegressionModel(String pathToMdl) {
        return loadDecisionTreeModel(pathToMdl);
    }

    /**
     * Load K-Means model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadKMeansModel(String pathToMdl) {
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
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

        return new KMeansModel(centers, new EuclideanDistance());
    }

    /**
     * Load model and its metadata from parquet files.
     *
     * @param pathToMdl Hadoop path to model saved from Spark.
     * @param pathToMetaData Hadoop path to metadata saved from Spark.
     * @param parsedSparkMdl One of supported Spark models to parse it.
     * @return Instance of parsedSparkMdl model.
     */
    public static Model parseWithMetadata(String pathToMdl, String pathToMetaData,
        SupportedSparkModels parsedSparkMdl) {
        File mdlRsrc1 = IgniteUtils.resolveIgnitePath(pathToMdl);
        if (mdlRsrc1 == null)
            throw new IllegalArgumentException("Resource not found [resource_path=" + pathToMdl + "]");

        String ignitePathToMdl = mdlRsrc1.getPath();

        File mdlRsrc2 = IgniteUtils.resolveIgnitePath(pathToMetaData);
        if (mdlRsrc2 == null)
            throw new IllegalArgumentException("Resource not found [resource_path=" + pathToMetaData + "]");

        String ignitePathToMdlMetaData = mdlRsrc2.getPath();

        switch (parsedSparkMdl) {
            case GRADIENT_BOOSTED_TREES:
                return loadGBTClassifierModel(ignitePathToMdl, ignitePathToMdlMetaData);
            case GRADIENT_BOOSTED_TREES_REGRESSION:
                return loadGBTRegressionModel(ignitePathToMdl, ignitePathToMdlMetaData);
            default:
                throw new UnsupportedSparkModelException(ignitePathToMdl);
        }
    }

    /**
     * Load GDB Regression model.
     *
     * @param pathToMdl Path to model.
     * @param pathToMdlMetaData Path to model meta data.
     */
    private static Model loadGBTRegressionModel(String pathToMdl, String pathToMdlMetaData) {
        IgniteFunction<Double, Double> lbMapper = lb -> lb;

        return parseAndBuildGDBModel(pathToMdl, pathToMdlMetaData, lbMapper);
    }

    /**
     * Load GDB Classification model.
     *
     * @param pathToMdl Path to model.
     * @param pathToMdlMetaData Path to model meta data.
     */
    private static Model loadGBTClassifierModel(String pathToMdl, String pathToMdlMetaData) {
        IgniteFunction<Double, Double> lbMapper = lb -> lb > 0.5 ? 1.0 : 0.0;

        return parseAndBuildGDBModel(pathToMdl, pathToMdlMetaData, lbMapper);
    }

    /**
     * Parse and build common GDB model with the custom label mapper.
     *
     * @param pathToMdl Path to model.
     * @param pathToMdlMetaData Path to model meta data.
     * @param lbMapper Label mapper.
     */
    @Nullable private static Model parseAndBuildGDBModel(String pathToMdl, String pathToMdlMetaData,
        IgniteFunction<Double, Double> lbMapper) {
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
            System.out.println("Error reading parquet file with MetaData by the path: " + pathToMdlMetaData);
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
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Load RF model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadRandomForestModel(String pathToMdl) {
        final List<IgniteModel<Vector, Double>> models = parseTreesForRandomForestAlgorithm(pathToMdl);
        if (models == null)
            return null;
        return new ModelsComposition(models, new OnMajorityPredictionsAggregator());
    }

    /**
     * Parse trees from file for common Random Forest ensemble.
     *
     * @param pathToMdl Path to model.
     */
    private static List<IgniteModel<Vector, Double>> parseTreesForRandomForestAlgorithm(String pathToMdl) {
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
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Load Decision Tree model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadDecisionTreeModel(String pathToMdl) {
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
            System.out.println("Error reading parquet file.");
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
     */
    private static Model loadLinearSVMModel(String pathToMdl) {
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
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

        return new SVMLinearClassificationModel(coefficients, interceptor);
    }

    /**
     * Load linear regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadLinRegModel(String pathToMdl) {
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
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

        return new LinearRegressionModel(coefficients, interceptor);
    }

    /**
     * Load logistic regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadLogRegModel(String pathToMdl) {
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
            System.out.println("Error reading parquet file.");
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
