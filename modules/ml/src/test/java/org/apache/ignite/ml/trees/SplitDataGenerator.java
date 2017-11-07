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
package org.apache.ignite.ml.trees;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.util.Utils;

/**
 * Utility class for generating data which has binary tree split structure.
 *
 * @param <V>
 */
public class SplitDataGenerator<V extends Vector> {
    /** */
    private static final double DELTA = 100.0;

    /** Map of the form of (is categorical -> list of region indexes). */
    private final Map<Boolean, List<Integer>> di;

    /** List of regions. */
    private final List<Region> regs;

    /** Data of bounds of regions. */
    private final Map<Integer, IgniteBiTuple<Double, Double>> boundsData;

    /** Random numbers generator. */
    private final Random rnd;

    /** Supplier of vectors. */
    private final Supplier<V> supplier;

    /** Features count. */
    private final int featCnt;

    /**
     * Create SplitDataGenerator.
     *
     * @param featCnt Features count.
     * @param catFeaturesInfo Information about categorical features in form of map (feature index -> categories
     * count).
     * @param supplier Supplier of vectors.
     * @param rnd Random numbers generator.
     */
    public SplitDataGenerator(int featCnt, Map<Integer, Integer> catFeaturesInfo, Supplier<V> supplier, Random rnd) {
        regs = new LinkedList<>();
        boundsData = new HashMap<>();
        this.rnd = rnd;
        this.supplier = supplier;
        this.featCnt = featCnt;

        // Divide indexes into indexes of categorical coordinates and indexes of continuous coordinates.
        di = IntStream.range(0, featCnt).
            boxed().
            collect(Collectors.partitioningBy(catFeaturesInfo::containsKey));

        // Categorical coordinates info.
        Map<Integer, CatCoordInfo> catCoords = new HashMap<>();
        di.get(true).forEach(i -> {
            BitSet bs = new BitSet();
            bs.set(0, catFeaturesInfo.get(i));
            catCoords.put(i, new CatCoordInfo(bs));
        });

        // Continous coordinates info.
        Map<Integer, ContCoordInfo> contCoords = new HashMap<>();
        di.get(false).forEach(i -> {
            contCoords.put(i, new ContCoordInfo());
            boundsData.put(i, new IgniteBiTuple<>(-1.0, 1.0));
        });

        Region firstReg = new Region(catCoords, contCoords, 0);
        regs.add(firstReg);
    }

    /**
     * Categorical coordinate info.
     */
    private static class CatCoordInfo implements Serializable {
        /**
         * Defines categories which are included in this region
         */
        private final BitSet bs;

        /**
         * Construct CatCoordInfo.
         *
         * @param bs Bitset.
         */
        public CatCoordInfo(BitSet bs) {
            this.bs = bs;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CatCoordInfo [" +
                "bs=" + bs +
                ']';
        }
    }

    /**
     * Continuous coordinate info.
     */
    private static class ContCoordInfo implements Serializable {
        /**
         * Left (min) bound of region.
         */
        private double left;

        /**
         * Right (max) bound of region.
         */
        private double right;

        /**
         * Construct ContCoordInfo.
         */
        public ContCoordInfo() {
            left = Double.NEGATIVE_INFINITY;
            right = Double.POSITIVE_INFINITY;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ContCoordInfo [" +
                "left=" + left +
                ", right=" + right +
                ']';
        }
    }

    /**
     * Class representing information about region.
     */
    private static class Region implements Serializable {
        /**
         * Information about categorical coordinates restrictions of this region in form of
         * (coordinate index -> restriction)
         */
        private final Map<Integer, CatCoordInfo> catCoords;

        /**
         * Information about continuous coordinates restrictions of this region in form of
         * (coordinate index -> restriction)
         */
        private final Map<Integer, ContCoordInfo> contCoords;

        /**
         * Region should contain {@code 1/2^twoPow * totalPoints} points.
         */
        private int twoPow;

        /**
         * Construct region by information about restrictions on coordinates (features) values.
         *
         * @param catCoords Restrictions on categorical coordinates.
         * @param contCoords Restrictions on continuous coordinates
         * @param twoPow Region should contain {@code 1/2^twoPow * totalPoints} points.
         */
        public Region(Map<Integer, CatCoordInfo> catCoords, Map<Integer, ContCoordInfo> contCoords, int twoPow) {
            this.catCoords = catCoords;
            this.contCoords = contCoords;
            this.twoPow = twoPow;
        }

        /** */
        public int divideBy() {
            return 1 << twoPow;
        }

        /** */
        public void incTwoPow() {
            twoPow++;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Region [" +
                "catCoords=" + catCoords +
                ", contCoords=" + contCoords +
                ", twoPow=" + twoPow +
                ']';
        }

        /**
         * Generate continuous coordinate for this region.
         *
         * @param coordIdx Coordinate index.
         * @param boundsData Data with bounds
         * @param rnd Random numbers generator.
         * @return Categorical coordinate value.
         */
        public double generateContCoord(int coordIdx, Map<Integer, IgniteBiTuple<Double, Double>> boundsData,
            Random rnd) {
            ContCoordInfo cci = contCoords.get(coordIdx);
            double left = cci.left;
            double right = cci.right;

            if (left == Double.NEGATIVE_INFINITY)
                left = boundsData.get(coordIdx).get1() - DELTA;

            if (right == Double.POSITIVE_INFINITY)
                right = boundsData.get(coordIdx).get2() + DELTA;

            double size = right - left;

            return left + rnd.nextDouble() * size;
        }

        /**
         * Generate categorical coordinate value for this region.
         *
         * @param coordIdx Coordinate index.
         * @param rnd Random numbers generator.
         * @return Categorical coordinate value.
         */
        public double generateCatCoord(int coordIdx, Random rnd) {
            // Pick random bit.
            BitSet bs = catCoords.get(coordIdx).bs;
            int j = rnd.nextInt(bs.length());

            int i = 0;
            int bn = 0;
            int bnp = 0;

            while ((bn = bs.nextSetBit(bn)) != -1 && i <= j) {
                i++;
                bnp = bn;
                bn++;
            }

            return bnp;
        }

        /**
         * Generate points for this region.
         *
         * @param ptsCnt Count of points to generate.
         * @param val Label for all points in this region.
         * @param boundsData Data about bounds of continuous coordinates.
         * @param catCont Data about which categories can be in this region in the form (coordinate index -> list of
         * categories indexes).
         * @param s Vectors supplier.
         * @param rnd Random numbers generator.
         * @param <V> Type of vectors.
         * @return Stream of generated points for this region.
         */
        public <V extends Vector> Stream<V> generatePoints(int ptsCnt, double val,
            Map<Integer, IgniteBiTuple<Double, Double>> boundsData, Map<Boolean, List<Integer>> catCont,
            Supplier<V> s,
            Random rnd) {
            return IntStream.range(0, ptsCnt / divideBy()).mapToObj(i -> {
                V v = s.get();
                int coordsCnt = v.size();
                catCont.get(false).forEach(ci -> v.setX(ci, generateContCoord(ci, boundsData, rnd)));
                catCont.get(true).forEach(ci -> v.setX(ci, generateCatCoord(ci, rnd)));

                v.setX(coordsCnt - 1, val);
                return v;
            });
        }
    }

    /**
     * Split region by continuous coordinate.using given threshold.
     *
     * @param regIdx Region index.
     * @param coordIdx Coordinate index.
     * @param threshold Threshold.
     * @return {@code this}.
     */
    public SplitDataGenerator<V> split(int regIdx, int coordIdx, double threshold) {
        Region regToSplit = regs.get(regIdx);
        ContCoordInfo cci = regToSplit.contCoords.get(coordIdx);

        double left = cci.left;
        double right = cci.right;

        if (threshold < left || threshold > right)
            throw new MathIllegalArgumentException("Threshold is out of region bounds.");

        regToSplit.incTwoPow();

        Region newReg = Utils.copy(regToSplit);
        newReg.contCoords.get(coordIdx).left = threshold;

        regs.add(regIdx + 1, newReg);
        cci.right = threshold;

        IgniteBiTuple<Double, Double> bounds = boundsData.get(coordIdx);
        double min = bounds.get1();
        double max = bounds.get2();
        boundsData.put(coordIdx, new IgniteBiTuple<>(Math.min(threshold, min), Math.max(max, threshold)));

        return this;
    }

    /**
     * Split region by categorical coordinate.
     *
     * @param regIdx Region index.
     * @param coordIdx Coordinate index.
     * @param cats Categories allowed for the left sub region.
     * @return {@code this}.
     */
    public SplitDataGenerator<V> split(int regIdx, int coordIdx, int[] cats) {
        BitSet subset = new BitSet();
        Arrays.stream(cats).forEach(subset::set);
        Region regToSplit = regs.get(regIdx);
        CatCoordInfo cci = regToSplit.catCoords.get(coordIdx);

        BitSet ssc = (BitSet)subset.clone();
        BitSet set = cci.bs;
        ssc.and(set);
        if (ssc.length() != subset.length())
            throw new MathIllegalArgumentException("Splitter set is not a subset of a parent subset.");

        ssc.xor(set);
        set.and(subset);

        regToSplit.incTwoPow();
        Region newReg = Utils.copy(regToSplit);
        newReg.catCoords.put(coordIdx, new CatCoordInfo(ssc));

        regs.add(regIdx + 1, newReg);

        return this;
    }

    /**
     * Get stream of points generated by this generator.
     *
     * @param ptsCnt Points count.
     */
    public Stream<IgniteBiTuple<Integer, V>> points(int ptsCnt, BiFunction<Double, Random, Double> f) {
        regs.forEach(System.out::println);

        return IntStream.range(0, regs.size()).
            boxed().
            map(i -> regs.get(i).generatePoints(ptsCnt, f.apply((double)i, rnd), boundsData, di, supplier, rnd).map(v -> new IgniteBiTuple<>(i, v))).flatMap(Function.identity());
    }

    /**
     * Count of regions.
     *
     * @return Count of regions.
     */
    public int regsCount() {
        return regs.size();
    }

    /**
     * Get features count.
     *
     * @return Features count.
     */
    public int featuresCnt() {
        return featCnt;
    }
}
