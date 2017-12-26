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

package org.apache.ignite.yardstick.ml.knn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Datasets used in KMeansDistributedClustererExample and in KMeansLocalClustererExample.
 */
class Datasets {
    /**
     * Generate dataset shuffled as defined in parameter.
     *
     * @param off Parameter to use for shuffling raw data.
     * @return Generated dataset.
     */
    LabeledDataset shuffleIris(int off) {
        return shuffle(off, vectorsIris, labelsIris);
    }

    /**
     * Generate dataset shuffled as defined in parameter.
     *
     * @param off Parameter to use for shuffling raw data.
     * @return Generated dataset.
     */
    LabeledDataset shuffleClearedMachines(int off) {
        return shuffle(off, vectorsClearedMachines, labelsClearedMachines);
    }

    /** */
    private LabeledDataset shuffle(int off, List<Vector> vectors, List<Double> labels) {
        int size = vectors.size();

        LabeledVector[] data = new LabeledVector[size];
        for (int i = 0; i < vectors.size(); i++)
            data[(i + off) % (size - 1)] = new LabeledVector<>(vectors.get(i), labels.get(i));

        return new LabeledDataset(data, vectors.get(0).size());
    }

    /** */
    private final static double[][] dataIris = {
        new double[] {1.0, 5.1, 3.5, 1.4, 0.2},
        new double[] {1.0, 4.9, 3.0, 1.4, 0.2},
        new double[] {1.0, 4.7, 3.2, 1.3, 0.2},
        new double[] {1.0, 4.6, 3.1, 1.5, 0.2},
        new double[] {1.0, 5.0, 3.6, 1.4, 0.2},
        new double[] {1.0, 5.4, 3.9, 1.7, 0.4},
        new double[] {1.0, 4.6, 3.4, 1.4, 0.3},
        new double[] {1.0, 5.0, 3.4, 1.5, 0.2},
        new double[] {1.0, 4.4, 2.9, 1.4, 0.2},
        new double[] {1.0, 4.9, 3.1, 1.5, 0.1},
        new double[] {1.0, 5.4, 3.7, 1.5, 0.2},
        new double[] {1.0, 4.8, 3.4, 1.6, 0.2},
        new double[] {1.0, 4.8, 3.0, 1.4, 0.1},
        new double[] {1.0, 4.3, 3.0, 1.1, 0.1},
        new double[] {1.0, 5.8, 4.0, 1.2, 0.2},
        new double[] {1.0, 5.7, 4.4, 1.5, 0.4},
        new double[] {1.0, 5.4, 3.9, 1.3, 0.4},
        new double[] {1.0, 5.1, 3.5, 1.4, 0.3},
        new double[] {1.0, 5.7, 3.8, 1.7, 0.3},
        new double[] {1.0, 5.1, 3.8, 1.5, 0.3},
        new double[] {1.0, 5.4, 3.4, 1.7, 0.2},
        new double[] {1.0, 5.1, 3.7, 1.5, 0.4},
        new double[] {1.0, 4.6, 3.6, 1.0, 0.2},
        new double[] {1.0, 5.1, 3.3, 1.7, 0.5},
        new double[] {1.0, 4.8, 3.4, 1.9, 0.2},
        new double[] {1.0, 5.0, 3.0, 1.6, 0.2},
        new double[] {1.0, 5.0, 3.4, 1.6, 0.4},
        new double[] {1.0, 5.2, 3.5, 1.5, 0.2},
        new double[] {1.0, 5.2, 3.4, 1.4, 0.2},
        new double[] {1.0, 4.7, 3.2, 1.6, 0.2},
        new double[] {1.0, 4.8, 3.1, 1.6, 0.2},
        new double[] {1.0, 5.4, 3.4, 1.5, 0.4},
        new double[] {1.0, 5.2, 4.1, 1.5, 0.1},
        new double[] {1.0, 5.5, 4.2, 1.4, 0.2},
        new double[] {1.0, 4.9, 3.1, 1.5, 0.1},
        new double[] {1.0, 5.0, 3.2, 1.2, 0.2},
        new double[] {1.0, 5.5, 3.5, 1.3, 0.2},
        new double[] {1.0, 4.9, 3.1, 1.5, 0.1},
        new double[] {1.0, 4.4, 3.0, 1.3, 0.2},
        new double[] {1.0, 5.1, 3.4, 1.5, 0.2},
        new double[] {1.0, 5.0, 3.5, 1.3, 0.3},
        new double[] {1.0, 4.5, 2.3, 1.3, 0.3},
        new double[] {1.0, 4.4, 3.2, 1.3, 0.2},
        new double[] {1.0, 5.0, 3.5, 1.6, 0.6},
        new double[] {1.0, 5.1, 3.8, 1.9, 0.4},
        new double[] {1.0, 4.8, 3.0, 1.4, 0.3},
        new double[] {1.0, 5.1, 3.8, 1.6, 0.2},
        new double[] {1.0, 4.6, 3.2, 1.4, 0.2},
        new double[] {1.0, 5.3, 3.7, 1.5, 0.2},
        new double[] {1.0, 5.0, 3.3, 1.4, 0.2},
        new double[] {2.0, 7.0, 3.2, 4.7, 1.4},
        new double[] {2.0, 6.4, 3.2, 4.5, 1.5},
        new double[] {2.0, 6.9, 3.1, 4.9, 1.5},
        new double[] {2.0, 5.5, 2.3, 4.0, 1.3},
        new double[] {2.0, 6.5, 2.8, 4.6, 1.5},
        new double[] {2.0, 5.7, 2.8, 4.5, 1.3},
        new double[] {2.0, 6.3, 3.3, 4.7, 1.6},
        new double[] {2.0, 4.9, 2.4, 3.3, 1.0},
        new double[] {2.0, 6.6, 2.9, 4.6, 1.3},
        new double[] {2.0, 5.2, 2.7, 3.9, 1.4},
        new double[] {2.0, 5.0, 2.0, 3.5, 1.0},
        new double[] {2.0, 5.9, 3.0, 4.2, 1.5},
        new double[] {2.0, 6.0, 2.2, 4.0, 1.0},
        new double[] {2.0, 6.1, 2.9, 4.7, 1.4},
        new double[] {2.0, 5.6, 2.9, 3.6, 1.3},
        new double[] {2.0, 6.7, 3.1, 4.4, 1.4},
        new double[] {2.0, 5.6, 3.0, 4.5, 1.5},
        new double[] {2.0, 5.8, 2.7, 4.1, 1.0},
        new double[] {2.0, 6.2, 2.2, 4.5, 1.5},
        new double[] {2.0, 5.6, 2.5, 3.9, 1.1},
        new double[] {2.0, 5.9, 3.2, 4.8, 1.8},
        new double[] {2.0, 6.1, 2.8, 4.0, 1.3},
        new double[] {2.0, 6.3, 2.5, 4.9, 1.5},
        new double[] {2.0, 6.1, 2.8, 4.7, 1.2},
        new double[] {2.0, 6.4, 2.9, 4.3, 1.3},
        new double[] {2.0, 6.6, 3.0, 4.4, 1.4},
        new double[] {2.0, 6.8, 2.8, 4.8, 1.4},
        new double[] {2.0, 6.7, 3.0, 5.0, 1.7},
        new double[] {2.0, 6.0, 2.9, 4.5, 1.5},
        new double[] {2.0, 5.7, 2.6, 3.5, 1.0},
        new double[] {2.0, 5.5, 2.4, 3.8, 1.1},
        new double[] {2.0, 5.5, 2.4, 3.7, 1.0},
        new double[] {2.0, 5.8, 2.7, 3.9, 1.2},
        new double[] {2.0, 6.0, 2.7, 5.1, 1.6},
        new double[] {2.0, 5.4, 3.0, 4.5, 1.5},
        new double[] {2.0, 6.0, 3.4, 4.5, 1.6},
        new double[] {2.0, 6.7, 3.1, 4.7, 1.5},
        new double[] {2.0, 6.3, 2.3, 4.4, 1.3},
        new double[] {2.0, 5.6, 3.0, 4.1, 1.3},
        new double[] {2.0, 5.5, 2.5, 4.0, 1.3},
        new double[] {2.0, 5.5, 2.6, 4.4, 1.2},
        new double[] {2.0, 6.1, 3.0, 4.6, 1.4},
        new double[] {2.0, 5.8, 2.6, 4.0, 1.2},
        new double[] {2.0, 5.0, 2.3, 3.3, 1.0},
        new double[] {2.0, 5.6, 2.7, 4.2, 1.3},
        new double[] {2.0, 5.7, 3.0, 4.2, 1.2},
        new double[] {2.0, 5.7, 2.9, 4.2, 1.3},
        new double[] {2.0, 6.2, 2.9, 4.3, 1.3},
        new double[] {2.0, 5.1, 2.5, 3.0, 1.1},
        new double[] {2.0, 5.7, 2.8, 4.1, 1.3},
        new double[] {3.0, 6.3, 3.3, 6.0, 2.5},
        new double[] {3.0, 5.8, 2.7, 5.1, 1.9},
        new double[] {3.0, 7.1, 3.0, 5.9, 2.1},
        new double[] {3.0, 6.3, 2.9, 5.6, 1.8},
        new double[] {3.0, 6.5, 3.0, 5.8, 2.2},
        new double[] {3.0, 7.6, 3.0, 6.6, 2.1},
        new double[] {3.0, 4.9, 2.5, 4.5, 1.7},
        new double[] {3.0, 7.3, 2.9, 6.3, 1.8},
        new double[] {3.0, 6.7, 2.5, 5.8, 1.8},
        new double[] {3.0, 7.2, 3.6, 6.1, 2.5},
        new double[] {3.0, 6.5, 3.2, 5.1, 2.0},
        new double[] {3.0, 6.4, 2.7, 5.3, 1.9},
        new double[] {3.0, 6.8, 3.0, 5.5, 2.1},
        new double[] {3.0, 5.7, 2.5, 5.0, 2.0},
        new double[] {3.0, 5.8, 2.8, 5.1, 2.4},
        new double[] {3.0, 6.4, 3.2, 5.3, 2.3},
        new double[] {3.0, 6.5, 3.0, 5.5, 1.8},
        new double[] {3.0, 7.7, 3.8, 6.7, 2.2},
        new double[] {3.0, 7.7, 2.6, 6.9, 2.3},
        new double[] {3.0, 6.0, 2.2, 5.0, 1.5},
        new double[] {3.0, 6.9, 3.2, 5.7, 2.3},
        new double[] {3.0, 5.6, 2.8, 4.9, 2.0},
        new double[] {3.0, 7.7, 2.8, 6.7, 2.0},
        new double[] {3.0, 6.3, 2.7, 4.9, 1.8},
        new double[] {3.0, 6.7, 3.3, 5.7, 2.1},
        new double[] {3.0, 7.2, 3.2, 6.0, 1.8},
        new double[] {3.0, 6.2, 2.8, 4.8, 1.8},
        new double[] {3.0, 6.1, 3.0, 4.9, 1.8},
        new double[] {3.0, 6.4, 2.8, 5.6, 2.1},
        new double[] {3.0, 7.2, 3.0, 5.8, 1.6},
        new double[] {3.0, 7.4, 2.8, 6.1, 1.9},
        new double[] {3.0, 7.9, 3.8, 6.4, 2.0},
        new double[] {3.0, 6.4, 2.8, 5.6, 2.2},
        new double[] {3.0, 6.3, 2.8, 5.1, 1.5},
        new double[] {3.0, 6.1, 2.6, 5.6, 1.4},
        new double[] {3.0, 7.7, 3.0, 6.1, 2.3},
        new double[] {3.0, 6.3, 3.4, 5.6, 2.4},
        new double[] {3.0, 6.4, 3.1, 5.5, 1.8},
        new double[] {3.0, 6.0, 3.0, 4.8, 1.8},
        new double[] {3.0, 6.9, 3.1, 5.4, 2.1},
        new double[] {3.0, 6.7, 3.1, 5.6, 2.4},
        new double[] {3.0, 6.9, 3.1, 5.1, 2.3},
        new double[] {3.0, 5.8, 2.7, 5.1, 1.9},
        new double[] {3.0, 6.8, 3.2, 5.9, 2.3},
        new double[] {3.0, 6.7, 3.3, 5.7, 2.5},
        new double[] {3.0, 6.7, 3.0, 5.2, 2.3},
        new double[] {3.0, 6.3, 2.5, 5.0, 1.9},
        new double[] {3.0, 6.5, 3.0, 5.2, 2.0},
        new double[] {3.0, 6.2, 3.4, 5.4, 2.3},
        new double[] {3.0, 5.9, 3.0, 5.1, 1.8},
    };

    /** */
    private static final List<Double> labelsIris = new ArrayList<>();

    /** */
    private static final List<Vector> vectorsIris = new ArrayList<>();

    /** */
    private final static double[][] dataClearedMachines = {
        new double[] {199,125,256,6000,256,16,128},
        new double[] {253,29,8000,32000,32,8,32},
        new double[] {253,29,8000,32000,32,8,32},
        new double[] {253,29,8000,32000,32,8,32},
        new double[] {132,29,8000,16000,32,8,16},
        new double[] {290,26,8000,32000,64,8,32},
        new double[] {381,23,16000,32000,64,16,32},
        new double[] {381,23,16000,32000,64,16,32},
        new double[] {749,23,16000,64000,64,16,32},
        new double[] {1238,23,32000,64000,128,32,64},
        new double[] {23,400,1000,3000,0,1,2},
        new double[] {24,400,512,3500,4,1,6},
        new double[] {70,60,2000,8000,65,1,8},
        new double[] {117,50,4000,16000,65,1,8},
        new double[] {15,350,64,64,0,1,4},
        new double[] {64,200,512,16000,0,4,32},
        new double[] {23,167,524,2000,8,4,15},
        new double[] {29,143,512,5000,0,7,32},
        new double[] {22,143,1000,2000,0,5,16},
        new double[] {124,110,5000,5000,142,8,64},
        new double[] {35,143,1500,6300,0,5,32},
        new double[] {39,143,3100,6200,0,5,20},
        new double[] {40,143,2300,6200,0,6,64},
        new double[] {45,110,3100,6200,0,6,64},
        new double[] {28,320,128,6000,0,1,12},
        new double[] {21,320,512,2000,4,1,3},
        new double[] {28,320,256,6000,0,1,6},
        new double[] {22,320,256,3000,4,1,3},
        new double[] {28,320,512,5000,4,1,5},
        new double[] {27,320,256,5000,4,1,6},
        new double[] {102,25,1310,2620,131,12,24},
        new double[] {102,25,1310,2620,131,12,24},
        new double[] {74,50,2620,10480,30,12,24},
        new double[] {74,50,2620,10480,30,12,24},
        new double[] {138,56,5240,20970,30,12,24},
        new double[] {136,64,5240,20970,30,12,24},
        new double[] {23,50,500,2000,8,1,4},
        new double[] {29,50,1000,4000,8,1,5},
        new double[] {44,50,2000,8000,8,1,5},
        new double[] {30,50,1000,4000,8,3,5},
        new double[] {41,50,1000,8000,8,3,5},
        new double[] {74,50,2000,16000,8,3,5},
        new double[] {74,50,2000,16000,8,3,6},
        new double[] {74,50,2000,16000,8,3,6},
        new double[] {54,133,1000,12000,9,3,12},
        new double[] {41,133,1000,8000,9,3,12},
        new double[] {18,810,512,512,8,1,1},
        new double[] {28,810,1000,5000,0,1,1},
        new double[] {36,320,512,8000,4,1,5},
        new double[] {38,200,512,8000,8,1,8},
        new double[] {34,700,384,8000,0,1,1},
        new double[] {19,700,256,2000,0,1,1},
        new double[] {72,140,1000,16000,16,1,3},
        new double[] {36,200,1000,8000,0,1,2},
        new double[] {30,110,1000,4000,16,1,2},
        new double[] {56,110,1000,12000,16,1,2},
        new double[] {42,220,1000,8000,16,1,2},
        new double[] {34,800,256,8000,0,1,4},
        new double[] {34,800,256,8000,0,1,4},
        new double[] {34,800,256,8000,0,1,4},
        new double[] {34,800,256,8000,0,1,4},
        new double[] {34,800,256,8000,0,1,4},
        new double[] {19,125,512,1000,0,8,20},
        new double[] {75,75,2000,8000,64,1,38},
        new double[] {113,75,2000,16000,64,1,38},
        new double[] {157,75,2000,16000,128,1,38},
        new double[] {18,90,256,1000,0,3,10},
        new double[] {20,105,256,2000,0,3,10},
        new double[] {28,105,1000,4000,0,3,24},
        new double[] {33,105,2000,4000,8,3,19},
        new double[] {47,75,2000,8000,8,3,24},
        new double[] {54,75,3000,8000,8,3,48},
        new double[] {20,175,256,2000,0,3,24},
        new double[] {23,300,768,3000,0,6,24},
        new double[] {25,300,768,3000,6,6,24},
        new double[] {52,300,768,12000,6,6,24},
        new double[] {27,300,768,4500,0,1,24},
        new double[] {50,300,384,12000,6,1,24},
        new double[] {18,300,192,768,6,6,24},
        new double[] {53,180,768,12000,6,1,31},
        new double[] {23,330,1000,3000,0,2,4},
        new double[] {30,300,1000,4000,8,3,64},
        new double[] {73,300,1000,16000,8,2,112},
        new double[] {20,330,1000,2000,0,1,2},
        new double[] {25,330,1000,4000,0,3,6},
        new double[] {28,140,2000,4000,0,3,6},
        new double[] {29,140,2000,4000,0,4,8},
        new double[] {32,140,2000,4000,8,1,20},
        new double[] {175,140,2000,32000,32,1,20},
        new double[] {57,140,2000,8000,32,1,54},
        new double[] {181,140,2000,32000,32,1,54},
        new double[] {181,140,2000,32000,32,1,54},
        new double[] {32,140,2000,4000,8,1,20},
        new double[] {82,57,4000,16000,1,6,12},
        new double[] {171,57,4000,24000,64,12,16},
        new double[] {361,26,16000,32000,64,16,24},
        new double[] {350,26,16000,32000,64,8,24},
        new double[] {220,26,8000,32000,0,8,24},
        new double[] {113,26,8000,16000,0,8,16},
        new double[] {15,480,96,512,0,1,1},
        new double[] {21,203,1000,2000,0,1,5},
        new double[] {35,115,512,6000,16,1,6},
        new double[] {18,1100,512,1500,0,1,1},
        new double[] {20,1100,768,2000,0,1,1},
        new double[] {20,600,768,2000,0,1,1},
        new double[] {28,400,2000,4000,0,1,1},
        new double[] {45,400,4000,8000,0,1,1},
        new double[] {18,900,1000,1000,0,1,2},
        new double[] {17,900,512,1000,0,1,2},
        new double[] {26,900,1000,4000,4,1,2},
        new double[] {28,900,1000,4000,8,1,2},
        new double[] {28,900,2000,4000,0,3,6},
        new double[] {31,225,2000,4000,8,3,6},
        new double[] {31,225,2000,4000,8,3,6},
        new double[] {42,180,2000,8000,8,1,6},
        new double[] {76,185,2000,16000,16,1,6},
        new double[] {76,180,2000,16000,16,1,6},
        new double[] {26,225,1000,4000,2,3,6},
        new double[] {59,25,2000,12000,8,1,4},
        new double[] {65,25,2000,12000,16,3,5},
        new double[] {101,17,4000,16000,8,6,12},
        new double[] {116,17,4000,16000,32,6,12},
        new double[] {18,1500,768,1000,0,0,0},
        new double[] {20,1500,768,2000,0,0,0},
        new double[] {20,800,768,2000,0,0,0},
        new double[] {30,50,2000,4000,0,3,6},
        new double[] {44,50,2000,8000,8,3,6},
        new double[] {44,50,2000,8000,8,1,6},
        new double[] {82,50,2000,16000,24,1,6},
        new double[] {82,50,2000,16000,24,1,6},
        new double[] {128,50,8000,16000,48,1,10},
        new double[] {37,100,1000,8000,0,2,6},
        new double[] {46,100,1000,8000,24,2,6},
        new double[] {46,100,1000,8000,24,3,6},
        new double[] {80,50,2000,16000,12,3,16},
        new double[] {88,50,2000,16000,24,6,16},
        new double[] {88,50,2000,16000,24,6,16},
        new double[] {33,150,512,4000,0,8,128},
        new double[] {46,115,2000,8000,16,1,3},
        new double[] {29,115,2000,4000,2,1,5},
        new double[] {53,92,2000,8000,32,1,6},
        new double[] {53,92,2000,8000,32,1,6},
        new double[] {41,92,2000,8000,4,1,6},
        new double[] {86,75,4000,16000,16,1,6},
        new double[] {95,60,4000,16000,32,1,6},
        new double[] {107,60,2000,16000,64,5,8},
        new double[] {117,60,4000,16000,64,5,8},
        new double[] {119,50,4000,16000,64,5,10},
        new double[] {120,72,4000,16000,64,8,16},
        new double[] {48,72,2000,8000,16,6,8},
        new double[] {126,40,8000,16000,32,8,16},
        new double[] {266,40,8000,32000,64,8,24},
        new double[] {270,35,8000,32000,64,8,24},
        new double[] {426,38,16000,32000,128,16,32},
        new double[] {151,48,4000,24000,32,8,24},
        new double[] {267,38,8000,32000,64,8,24},
        new double[] {603,30,16000,32000,256,16,24},
        new double[] {19,112,1000,1000,0,1,4},
        new double[] {21,84,1000,2000,0,1,6},
        new double[] {26,56,1000,4000,0,1,6},
        new double[] {35,56,2000,6000,0,1,8},
        new double[] {41,56,2000,8000,0,1,8},
        new double[] {47,56,4000,8000,0,1,8},
        new double[] {62,56,4000,12000,0,1,8},
        new double[] {78,56,4000,16000,0,1,8},
        new double[] {80,38,4000,8000,32,16,32},
        new double[] {80,38,4000,8000,32,16,32},
        new double[] {142,38,8000,16000,64,4,8},
        new double[] {281,38,8000,24000,160,4,8},
        new double[] {190,38,4000,16000,128,16,32},
        new double[] {21,200,1000,2000,0,1,2},
        new double[] {25,200,1000,4000,0,1,4},
        new double[] {67,200,2000,8000,64,1,5},
        new double[] {24,250,512,4000,0,1,7},
        new double[] {24,250,512,4000,0,4,7},
        new double[] {64,250,1000,16000,1,1,8},
        new double[] {25,160,512,4000,2,1,5},
        new double[] {20,160,512,2000,2,3,8},
        new double[] {29,160,1000,4000,8,1,14},
        new double[] {43,160,1000,8000,16,1,14},
        new double[] {53,160,2000,8000,32,1,13},
        new double[] {19,240,512,1000,8,1,3},
        new double[] {22,240,512,2000,8,1,5},
        new double[] {31,105,2000,4000,8,3,8},
        new double[] {41,105,2000,6000,16,6,16},
        new double[] {47,105,2000,8000,16,4,14},
        new double[] {99,52,4000,16000,32,4,12},
        new double[] {67,70,4000,12000,8,6,8},
        new double[] {81,59,4000,12000,32,6,12},
        new double[] {149,59,8000,16000,64,12,24},
        new double[] {183,26,8000,24000,32,8,16},
        new double[] {275,26,8000,32000,64,12,16},
        new double[] {382,26,8000,32000,128,24,32},
        new double[] {56,116,2000,8000,32,5,28},
        new double[] {182,50,2000,32000,24,6,26},
        new double[] {227,50,2000,32000,48,26,52},
        new double[] {341,50,2000,32000,112,52,104},
        new double[] {360,50,4000,32000,112,52,104},
        new double[] {919,30,8000,64000,96,12,176},
        new double[] {978,30,8000,64000,128,12,176},
        new double[] {24,180,262,4000,0,1,3},
        new double[] {24,180,512,4000,0,1,3},
        new double[] {24,180,262,4000,0,1,3},
        new double[] {24,180,512,4000,0,1,3},
        new double[] {37,124,1000,8000,0,1,8},
        new double[] {50,98,1000,8000,32,2,8},
        new double[] {41,125,2000,8000,0,2,14},
        new double[] {47,480,512,8000,32,0,0},
        new double[] {25,480,1000,4000,0,0,0},
    };

    /** */
    private static final List<Double> labelsClearedMachines = new ArrayList<>();

    /** */
    private static final List<Vector> vectorsClearedMachines = new ArrayList<>();

    static {
        Arrays.stream(dataIris).forEachOrdered(e -> {
            labelsIris.add(e[0]);
            vectorsIris.add(new DenseLocalOnHeapVector(new double[] {e[1], e[2], e[3], e[4]}));
        });

        Arrays.stream(dataClearedMachines).forEachOrdered(e -> {
            labelsClearedMachines.add(e[0]);
            vectorsClearedMachines.add(new DenseLocalOnHeapVector(new double[] {e[1], e[2], e[3], e[4]}));
        });
    }
}
