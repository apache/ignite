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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

/**
 * Utility class for generating data which has binary tree split structure.
 * @param <V>
 */
public class SplitDataGenerator<V extends Vector> {
    private static final double DELTA = 100.0;

    /** Map of the form of (is categorical -> list of region indexes). */
    private final Map<Boolean, List<Integer>> di;

    /** List of regions. */
    private List<Region> regs;

    /** Data of bounds of regions. */
    private Map<Integer, IgniteBiTuple<Double, Double>> boundsData;

    /** Random numbers generator. */
    private Random rnd;

    /** Supplier of vectors. */
    private Supplier<V> supplier;

    /** Features count. */
    private int featCnt;

    /**
     * Create SplitDataGenerator.
     * @param featCnt Features count.
     * @param catFeaturesInfo Information about categorical features in form of map (feature index -> categories count).
     * @param supplier Supplier of vectors.
     * @param rnd Random numbers generator.
     */
    public SplitDataGenerator(int featCnt, Map<Integer, Integer> catFeaturesInfo, Supplier<V> supplier, Random rnd) {
        regs = new LinkedList<>();
        boundsData = new HashMap<>();
        this.rnd = rnd;
        this.supplier = supplier;
        this.featCnt = featCnt;

        // Divide indexes into indexes of categorical coordinates and indexes of continous coordinates.
        di = IntStream.range(0, featCnt).
            boxed().
            collect(Collectors.partitioningBy(catFeaturesInfo::containsKey));

        // Categorical coordinates info.
        Map<Integer, CatCoordInfo> catCoords = new HashMap<>();
        di.get(true).stream().forEach(i -> {
            BitSet bs = new BitSet();
            bs.set(0, catFeaturesInfo.get(i));
            catCoords.put(i, new CatCoordInfo(bs));
        });

        // Continous coordinates info.
        Map<Integer, ContCoordInfo> contCoords = new HashMap<>();
        di.get(false).stream().forEach(i -> {
            contCoords.put(i, new ContCoordInfo());
            boundsData.put(i, new IgniteBiTuple<>(-1.0, 1.0));
        });

        Region firstReg = new Region(catCoords, contCoords, 0);
        regs.add(firstReg);
    }

    /**
     * Categorical coordinate info
     */
    private static class CatCoordInfo implements Serializable {
        private BitSet bs;

        public CatCoordInfo(BitSet bs) {
            this.bs = bs;
        }

        @Override public String toString() {
            return "CatCoordInfo [" +
                "bs=" + bs +
                ']';
        }
    }

    /**
     *
     */
    private static class ContCoordInfo implements Serializable {
        private double left;
        private double right;

        public ContCoordInfo() {
            left = Double.NEGATIVE_INFINITY;
            right = Double.POSITIVE_INFINITY;
        }

        @Override public String toString() {
            return "ContCoordInfo [" +
                "left=" + left +
                ", right=" + right +
                ']';
        }
    }

    private static class Region implements Serializable {
        private Map<Integer, CatCoordInfo> catCoords;
        private Map<Integer, ContCoordInfo> contCoords;
        private int twoPow;

        public Region(Map<Integer, CatCoordInfo> catCoords, Map<Integer, ContCoordInfo> contCoords, int twoPow) {
            this.catCoords = catCoords;
            this.contCoords = contCoords;
            this.twoPow = twoPow;
        }

        public int divideBy() {
            return 1 << twoPow;
        }

        public void incTwoPow() {
            twoPow++;
        }

        @Override public String toString() {
            return "Region [" +
                "catCoords=" + catCoords +
                ", contCoords=" + contCoords +
                ", twoPow=" + twoPow +
                ']';
        }

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

    public SplitDataGenerator<V> split(int regIdx, int coordIdx, double threshold) {
        Region regToSplit = regs.get(regIdx);
        ContCoordInfo cci = regToSplit.contCoords.get(coordIdx);

        double left = cci.left;
        double right = cci.right;

        if (threshold < left || threshold > right)
            throw new MathIllegalArgumentException("Threshold is out of region bounds.");

        regToSplit.incTwoPow();

        Region newReg = copy(regToSplit);
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
     * Split region by coordinate.
     * @param regIdx Region index.
     * @param coordIdx Coordinate index.
     * @param cats
     * @return
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
        Region newReg = copy(regToSplit);
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
        regs.stream().forEach(System.out::println);

        return IntStream.range(0, regs.size()).
            boxed().
            map(i -> regs.get(i).generatePoints(ptsCnt, f.apply((double)i, rnd), boundsData, di, supplier, rnd).map(v -> new IgniteBiTuple<>(i, v))).flatMap(Function.identity());
    }

    /**
     * Count of regions.
     * @return Count of regions.
     */
    public int regsCount() {
        return regs.size();
    }

    public static <T> T copy(T orig) {
        Object obj = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(orig);
            out.flush();
            out.close();
            ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
            obj = in.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return (T)obj;
    }

    /**
     * Get features count.
     * @return Features count.
     */
    public int featuresCnt() {
        return featCnt;
    }
}
