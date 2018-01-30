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

package org.apache.ignite.ml.trees.trainers.columnbased;

import com.zaxxer.sparsebits.SparseBitSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteCurriedBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.models.DecisionTreeModel;
import org.apache.ignite.ml.trees.nodes.DecisionTreeNode;
import org.apache.ignite.ml.trees.nodes.Leaf;
import org.apache.ignite.ml.trees.nodes.SplitNode;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.ContextCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.FeaturesCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.FeaturesCache.FeatureKey;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.ProjectionsCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.ProjectionsCache.RegionKey;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.SplitCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.SplitCache.SplitKey;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.FeatureProcessor;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.ml.trees.trainers.columnbased.caches.FeaturesCache.getFeatureCacheKey;

/**
 * This trainer stores observations as columns and features as rows.
 * Ideas from https://github.com/fabuzaid21/yggdrasil are used here.
 */
public class ColumnDecisionTreeTrainer<D extends ContinuousRegionInfo> implements
    Trainer<DecisionTreeModel, ColumnDecisionTreeTrainerInput> {
    /**
     * Function used to assign a value to a region.
     */
    private final IgniteFunction<DoubleStream, Double> regCalc;

    /**
     * Function used to calculate impurity in regions used by categorical features.
     */
    private final IgniteFunction<ColumnDecisionTreeTrainerInput, ? extends ContinuousSplitCalculator<D>> continuousCalculatorProvider;

    /**
     * Categorical calculator provider.
     **/
    private final IgniteFunction<ColumnDecisionTreeTrainerInput, IgniteFunction<DoubleStream, Double>> categoricalCalculatorProvider;

    /**
     * Cache used for storing data for training.
     */
    private IgniteCache<RegionKey, List<RegionProjection>> prjsCache;

    /**
     * Minimal information gain.
     */
    private static final double MIN_INFO_GAIN = 1E-10;

    /**
     * Maximal depth of the decision tree.
     */
    private final int maxDepth;

    /**
     * Size of block which is used for storing regions in cache.
     */
    private static final int BLOCK_SIZE = 1 << 4;

    /** Ignite instance. */
    private final Ignite ignite;

    /** Logger */
    private final IgniteLogger log;

    /**
     * Construct {@link ColumnDecisionTreeTrainer}.
     *
     * @param maxDepth Maximal depth of the decision tree.
     * @param continuousCalculatorProvider Provider of calculator of splits for region projection on continuous
     * features.
     * @param categoricalCalculatorProvider Provider of calculator of splits for region projection on categorical
     * features.
     * @param regCalc Function used to assign a value to a region.
     */
    public ColumnDecisionTreeTrainer(int maxDepth,
        IgniteFunction<ColumnDecisionTreeTrainerInput, ? extends ContinuousSplitCalculator<D>> continuousCalculatorProvider,
        IgniteFunction<ColumnDecisionTreeTrainerInput, IgniteFunction<DoubleStream, Double>> categoricalCalculatorProvider,
        IgniteFunction<DoubleStream, Double> regCalc,
        Ignite ignite) {
        this.maxDepth = maxDepth;
        this.continuousCalculatorProvider = continuousCalculatorProvider;
        this.categoricalCalculatorProvider = categoricalCalculatorProvider;
        this.regCalc = regCalc;
        this.ignite = ignite;
        this.log = ignite.log();
    }

    /**
     * Utility class used to get index of feature by which split is done and split info.
     */
    private static class IndexAndSplitInfo {
        /**
         * Index of feature by which split is done.
         */
        private final int featureIdx;

        /**
         * Split information.
         */
        private final SplitInfo info;

        /**
         * @param featureIdx Index of feature by which split is done.
         * @param info Split information.
         */
        IndexAndSplitInfo(int featureIdx, SplitInfo info) {
            this.featureIdx = featureIdx;
            this.info = info;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "IndexAndSplitInfo [featureIdx=" + featureIdx + ", info=" + info + ']';
        }
    }

    /**
     * Utility class used to build decision tree. Basically it is pointer to leaf node.
     */
    private static class TreeTip {
        /** */
        private Consumer<DecisionTreeNode> leafSetter;

        /** */
        private int depth;

        /** */
        TreeTip(Consumer<DecisionTreeNode> leafSetter, int depth) {
            this.leafSetter = leafSetter;
            this.depth = depth;
        }
    }

    /**
     * Utility class used as decision tree root node.
     */
    private static class RootNode implements DecisionTreeNode {
        /** */
        private DecisionTreeNode s;

        /**
         * {@inheritDoc}
         */
        @Override public double process(Vector v) {
            return s.process(v);
        }

        /** */
        void setSplit(DecisionTreeNode s) {
            this.s = s;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public DecisionTreeModel train(ColumnDecisionTreeTrainerInput i) {
        prjsCache = ProjectionsCache.getOrCreate(ignite);
        IgniteCache<UUID, TrainingContext<D>> ctxtCache = ContextCache.getOrCreate(ignite);
        SplitCache.getOrCreate(ignite);

        UUID trainingUUID = UUID.randomUUID();

        TrainingContext<D> ct = new TrainingContext<>(i, continuousCalculatorProvider.apply(i), categoricalCalculatorProvider.apply(i), trainingUUID, ignite);
        ctxtCache.put(trainingUUID, ct);

        CacheUtils.bcast(prjsCache.getName(), ignite, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<RegionKey, List<RegionProjection>> projCache = ProjectionsCache.getOrCreate(ignite);
            IgniteCache<FeatureKey, double[]> featuresCache = FeaturesCache.getOrCreate(ignite);

            Affinity<RegionKey> targetAffinity = ignite.affinity(ProjectionsCache.CACHE_NAME);

            ClusterNode locNode = ignite.cluster().localNode();

            Map<FeatureKey, double[]> fm = new ConcurrentHashMap<>();
            Map<RegionKey, List<RegionProjection>> pm = new ConcurrentHashMap<>();

            targetAffinity.
                mapKeysToNodes(IntStream.range(0, i.featuresCount()).
                    mapToObj(idx -> ProjectionsCache.key(idx, 0, i.affinityKey(idx, ignite), trainingUUID)).
                    collect(Collectors.toSet())).getOrDefault(locNode, Collections.emptyList()).
                forEach(k -> {
                    FeatureProcessor vec;

                    int featureIdx = k.featureIdx();

                    IgniteCache<UUID, TrainingContext<D>> ctxCache = ContextCache.getOrCreate(ignite);
                    TrainingContext ctx = ctxCache.get(trainingUUID);
                    double[] vals = new double[ctx.labels().length];

                    vec = ctx.featureProcessor(featureIdx);
                    i.values(featureIdx).forEach(t -> vals[t.get1()] = t.get2());

                    fm.put(getFeatureCacheKey(featureIdx, trainingUUID, i.affinityKey(featureIdx, ignite)), vals);

                    List<RegionProjection> newReg = new ArrayList<>(BLOCK_SIZE);
                    newReg.add(vec.createInitialRegion(getSamples(i.values(featureIdx), ctx.labels().length), vals, ctx.labels()));
                    pm.put(k, newReg);
                });

            featuresCache.putAll(fm);
            projCache.putAll(pm);

            return null;
        });

        return doTrain(i, trainingUUID);
    }

    /**
     * Get samples array.
     *
     * @param values Stream of tuples in the form of (index, value).
     * @param size size of stream.
     * @return Samples array.
     */
    private Integer[] getSamples(Stream<IgniteBiTuple<Integer, Double>> values, int size) {
        Integer[] res = new Integer[size];

        values.forEach(v -> res[v.get1()] = v.get1());

        return res;
    }

    /** */
    @NotNull
    private DecisionTreeModel doTrain(ColumnDecisionTreeTrainerInput input, UUID uuid) {
        RootNode root = new RootNode();

        // List containing setters of leaves of the tree.
        List<TreeTip> tips = new LinkedList<>();
        tips.add(new TreeTip(root::setSplit, 0));

        int curDepth = 0;
        int regsCnt = 1;

        int featuresCnt = input.featuresCount();
        IntStream.range(0, featuresCnt).mapToObj(fIdx -> SplitCache.key(fIdx, input.affinityKey(fIdx, ignite), uuid)).
            forEach(k -> SplitCache.getOrCreate(ignite).put(k, new IgniteBiTuple<>(0, 0.0)));
        updateSplitCache(0, regsCnt, featuresCnt, ig -> i -> input.affinityKey(i, ig), uuid);

        // TODO: IGNITE-5893 Currently if the best split makes tree deeper than max depth process will be terminated, but actually we should
        // only stop when *any* improving split makes tree deeper than max depth. Can be fixed if we will store which
        // regions cannot be split more and split only those that can.
        while (true) {
            long before = System.currentTimeMillis();

            IgniteBiTuple<Integer, IgniteBiTuple<Integer, Double>> b = findBestSplitIndexForFeatures(featuresCnt, input::affinityKey, uuid);

            long findBestRegIdx = System.currentTimeMillis() - before;

            Integer bestFeatureIdx = b.get1();

            Integer regIdx = b.get2().get1();
            Double bestInfoGain = b.get2().get2();

            if (regIdx >= 0 && bestInfoGain > MIN_INFO_GAIN) {
                before = System.currentTimeMillis();

                SplitInfo bi = ignite.compute().affinityCall(ProjectionsCache.CACHE_NAME,
                    input.affinityKey(bestFeatureIdx, ignite),
                    () -> {
                        TrainingContext<ContinuousRegionInfo> ctx = ContextCache.getOrCreate(ignite).get(uuid);
                        Ignite ignite = Ignition.localIgnite();
                        RegionKey key = ProjectionsCache.key(bestFeatureIdx,
                            regIdx / BLOCK_SIZE,
                            input.affinityKey(bestFeatureIdx, Ignition.localIgnite()),
                            uuid);
                        RegionProjection reg = ProjectionsCache.getOrCreate(ignite).localPeek(key).get(regIdx % BLOCK_SIZE);
                        return ctx.featureProcessor(bestFeatureIdx).findBestSplit(reg, ctx.values(bestFeatureIdx, ignite), ctx.labels(), regIdx);
                    });

                long findBestSplit = System.currentTimeMillis() - before;

                IndexAndSplitInfo best = new IndexAndSplitInfo(bestFeatureIdx, bi);

                regsCnt++;

                if (log.isDebugEnabled())
                    log.debug("Globally best: " + best.info + " idx time: " + findBestRegIdx + ", calculate best: " + findBestSplit + " fi: " + best.featureIdx + ", regs: " + regsCnt);
                // Request bitset for split region.
                int ind = best.info.regionIndex();

                SparseBitSet bs = ignite.compute().affinityCall(ProjectionsCache.CACHE_NAME,
                    input.affinityKey(bestFeatureIdx, ignite),
                    () -> {
                        Ignite ignite = Ignition.localIgnite();
                        IgniteCache<FeatureKey, double[]> featuresCache = FeaturesCache.getOrCreate(ignite);
                        IgniteCache<UUID, TrainingContext<D>> ctxCache = ContextCache.getOrCreate(ignite);
                        TrainingContext ctx = ctxCache.localPeek(uuid);

                        double[] values = featuresCache.localPeek(getFeatureCacheKey(bestFeatureIdx, uuid, input.affinityKey(bestFeatureIdx, Ignition.localIgnite())));
                        RegionKey key = ProjectionsCache.key(bestFeatureIdx,
                            regIdx / BLOCK_SIZE,
                            input.affinityKey(bestFeatureIdx, Ignition.localIgnite()),
                            uuid);
                        RegionProjection reg = ProjectionsCache.getOrCreate(ignite).localPeek(key).get(regIdx % BLOCK_SIZE);
                        return ctx.featureProcessor(bestFeatureIdx).calculateOwnershipBitSet(reg, values, best.info);

                    });

                SplitNode sn = best.info.createSplitNode(best.featureIdx);

                TreeTip tipToSplit = tips.get(ind);
                tipToSplit.leafSetter.accept(sn);
                tipToSplit.leafSetter = sn::setLeft;
                int d = tipToSplit.depth++;
                tips.add(new TreeTip(sn::setRight, d));

                if (d > curDepth) {
                    curDepth = d;
                    if (log.isDebugEnabled()) {
                        log.debug("Depth: " + curDepth);
                        log.debug("Cache size: " + prjsCache.size(CachePeekMode.PRIMARY));
                    }
                }

                before = System.currentTimeMillis();
                // Perform split on all feature vectors.
                IgniteSupplier<Set<RegionKey>> bestRegsKeys = () -> IntStream.range(0, featuresCnt).
                    mapToObj(fIdx -> ProjectionsCache.key(fIdx, ind / BLOCK_SIZE, input.affinityKey(fIdx, Ignition.localIgnite()), uuid)).
                    collect(Collectors.toSet());

                int rc = regsCnt;

                // Perform split.
                CacheUtils.update(prjsCache.getName(), ignite,
                    (Ignite ign, Cache.Entry<RegionKey, List<RegionProjection>> e) -> {
                        RegionKey k = e.getKey();

                        List<RegionProjection> leftBlock = e.getValue();

                        int fIdx = k.featureIdx();
                        int idxInBlock = ind % BLOCK_SIZE;

                        IgniteCache<UUID, TrainingContext<D>> ctxCache = ContextCache.getOrCreate(ign);
                        TrainingContext<D> ctx = ctxCache.get(uuid);

                        RegionProjection targetRegProj = leftBlock.get(idxInBlock);

                        IgniteBiTuple<RegionProjection, RegionProjection> regs = ctx.
                            performSplit(input, bs, fIdx, best.featureIdx, targetRegProj, best.info.leftData(), best.info.rightData(), ign);

                        RegionProjection left = regs.get1();
                        RegionProjection right = regs.get2();

                        leftBlock.set(idxInBlock, left);
                        RegionKey rightKey = ProjectionsCache.key(fIdx, (rc - 1) / BLOCK_SIZE, input.affinityKey(fIdx, ign), uuid);

                        IgniteCache<RegionKey, List<RegionProjection>> c = ProjectionsCache.getOrCreate(ign);

                        List<RegionProjection> rightBlock = rightKey.equals(k) ? leftBlock : c.localPeek(rightKey);

                        if (rightBlock == null) {
                            List<RegionProjection> newBlock = new ArrayList<>(BLOCK_SIZE);
                            newBlock.add(right);
                            return Stream.of(new CacheEntryImpl<>(k, leftBlock), new CacheEntryImpl<>(rightKey, newBlock));
                        }
                        else {
                            rightBlock.add(right);
                            return rightBlock.equals(k) ?
                                Stream.of(new CacheEntryImpl<>(k, leftBlock)) :
                                Stream.of(new CacheEntryImpl<>(k, leftBlock), new CacheEntryImpl<>(rightKey, rightBlock));
                        }
                    },
                    bestRegsKeys);

                if (log.isDebugEnabled())
                    log.debug("Update of projections cache time: " + (System.currentTimeMillis() - before));

                before = System.currentTimeMillis();

                updateSplitCache(ind, rc, featuresCnt, ig -> i -> input.affinityKey(i, ig), uuid);

                if (log.isDebugEnabled())
                    log.debug("Update of split cache time: " + (System.currentTimeMillis() - before));
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Best split [bestFeatureIdx=" + bestFeatureIdx + ", bestInfoGain=" + bestInfoGain + "]");
                break;
            }
        }

        int rc = regsCnt;

        IgniteSupplier<Iterable<Cache.Entry<RegionKey, List<RegionProjection>>>> featZeroRegs = () -> {
            IgniteCache<RegionKey, List<RegionProjection>> projsCache = ProjectionsCache.getOrCreate(Ignition.localIgnite());

            return () -> IntStream.range(0, (rc - 1) / BLOCK_SIZE + 1).
                mapToObj(rBIdx -> ProjectionsCache.key(0, rBIdx, input.affinityKey(0, Ignition.localIgnite()), uuid)).
                map(k -> (Cache.Entry<RegionKey, List<RegionProjection>>)new CacheEntryImpl<>(k, projsCache.localPeek(k))).iterator();
        };

        Map<Integer, Double> vals = CacheUtils.reduce(prjsCache.getName(), ignite,
            (TrainingContext ctx, Cache.Entry<RegionKey, List<RegionProjection>> e, Map<Integer, Double> m) -> {
                int regBlockIdx = e.getKey().regionBlockIndex();

                if (e.getValue() != null) {
                    for (int i = 0; i < e.getValue().size(); i++) {
                        int regIdx = regBlockIdx * BLOCK_SIZE + i;
                        RegionProjection reg = e.getValue().get(i);

                        Double res = regCalc.apply(Arrays.stream(reg.sampleIndexes()).mapToDouble(s -> ctx.labels()[s]));
                        m.put(regIdx, res);
                    }
                }

                return m;
            },
            () -> ContextCache.getOrCreate(Ignition.localIgnite()).get(uuid),
            featZeroRegs,
            (infos, infos2) -> {
                Map<Integer, Double> res = new HashMap<>();
                res.putAll(infos);
                res.putAll(infos2);
                return res;
            },
            HashMap::new
        );

        int i = 0;
        for (TreeTip tip : tips) {
            tip.leafSetter.accept(new Leaf(vals.get(i)));
            i++;
        }

        ProjectionsCache.clear(featuresCnt, rc, input::affinityKey, uuid, ignite);
        ContextCache.getOrCreate(ignite).remove(uuid);
        FeaturesCache.clear(featuresCnt, input::affinityKey, uuid, ignite);
        SplitCache.clear(featuresCnt, input::affinityKey, uuid, ignite);

        return new DecisionTreeModel(root.s);
    }

    /**
     * Find the best split in the form (feature index, (index of region with the best split, impurity of region with the
     * best split)).
     *
     * @param featuresCnt Count of features.
     * @param affinity Affinity function.
     * @param trainingUUID UUID of training.
     * @return Best split in the form (feature index, (index of region with the best split, impurity of region with the
     * best split)).
     */
    private IgniteBiTuple<Integer, IgniteBiTuple<Integer, Double>> findBestSplitIndexForFeatures(int featuresCnt,
        IgniteBiFunction<Integer, Ignite, Object> affinity,
        UUID trainingUUID) {
        Set<Integer> featureIndexes = IntStream.range(0, featuresCnt).boxed().collect(Collectors.toSet());

        return CacheUtils.reduce(SplitCache.CACHE_NAME, ignite,
            (Object ctx, Cache.Entry<SplitKey, IgniteBiTuple<Integer, Double>> e, IgniteBiTuple<Integer, IgniteBiTuple<Integer, Double>> r) ->
                Functions.MAX_GENERIC(new IgniteBiTuple<>(e.getKey().featureIdx(), e.getValue()), r, comparator()),
            () -> null,
            () -> SplitCache.localEntries(featureIndexes, affinity, trainingUUID),
            (i1, i2) -> Functions.MAX_GENERIC(i1, i2, Comparator.comparingDouble(bt -> bt.get2().get2())),
            () -> new IgniteBiTuple<>(-1, new IgniteBiTuple<>(-1, Double.NEGATIVE_INFINITY))
        );
    }

    /** */
    private static Comparator<IgniteBiTuple<Integer, IgniteBiTuple<Integer, Double>>> comparator() {
        return Comparator.comparingDouble(bt -> bt != null && bt.get2() != null ? bt.get2().get2() : Double.NEGATIVE_INFINITY);
    }

    /**
     * Update split cache.
     *
     * @param lastSplitRegionIdx Index of region which had last best split.
     * @param regsCnt Count of regions.
     * @param featuresCnt Count of features.
     * @param affinity Affinity function.
     * @param trainingUUID UUID of current training.
     */
    private void updateSplitCache(int lastSplitRegionIdx, int regsCnt, int featuresCnt,
        IgniteCurriedBiFunction<Ignite, Integer, Object> affinity,
        UUID trainingUUID) {
        CacheUtils.update(SplitCache.CACHE_NAME, ignite,
            (Ignite ign, Cache.Entry<SplitKey, IgniteBiTuple<Integer, Double>> e) -> {
                Integer bestRegIdx = e.getValue().get1();
                int fIdx = e.getKey().featureIdx();
                TrainingContext ctx = ContextCache.getOrCreate(ign).get(trainingUUID);

                Map<Integer, RegionProjection> toCompare;

                // Fully recalculate best.
                if (bestRegIdx == lastSplitRegionIdx)
                    toCompare = ProjectionsCache.projectionsOfFeature(fIdx, maxDepth, regsCnt, BLOCK_SIZE, affinity.apply(ign), trainingUUID, ign);
                    // Just compare previous best and two regions which are produced by split.
                else
                    toCompare = ProjectionsCache.projectionsOfRegions(fIdx, maxDepth,
                        IntStream.of(bestRegIdx, lastSplitRegionIdx, regsCnt - 1), BLOCK_SIZE, affinity.apply(ign), trainingUUID, ign);

                double[] values = ctx.values(fIdx, ign);
                double[] labels = ctx.labels();

                Optional<IgniteBiTuple<Integer, Double>> max = toCompare.entrySet().stream().
                    map(ent -> {
                        SplitInfo bestSplit = ctx.featureProcessor(fIdx).findBestSplit(ent.getValue(), values, labels, ent.getKey());
                        return new IgniteBiTuple<>(ent.getKey(), bestSplit != null ? bestSplit.infoGain() : Double.NEGATIVE_INFINITY);
                    }).
                    max(Comparator.comparingDouble(IgniteBiTuple::get2));

                return max.<Stream<Cache.Entry<SplitKey, IgniteBiTuple<Integer, Double>>>>
                    map(objects -> Stream.of(new CacheEntryImpl<>(e.getKey(), objects))).orElseGet(Stream::empty);
            },
            () -> IntStream.range(0, featuresCnt).mapToObj(fIdx -> SplitCache.key(fIdx, affinity.apply(ignite).apply(fIdx), trainingUUID)).collect(Collectors.toSet())
        );
    }
}
