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

package org.apache.ignite.ml.util;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

public class MnistUtils {
    public static Stream<DenseLocalOnHeapVector> mnist(String imgPath, String labelsPath, Random rnd, int cnt) throws IOException {
        FileInputStream isImages = new FileInputStream(imgPath);
        FileInputStream isLabels = new FileInputStream(labelsPath);

        int magic = read4Bytes(isImages); // Skip magic number.
        int numOfImages = read4Bytes(isImages);
        int imgHeight = read4Bytes(isImages);
        int imgWidth = read4Bytes(isImages);

        read4Bytes(isLabels); // Skip magic number.
        read4Bytes(isLabels); // Skip number of labels.

        int numOfPixels = imgHeight * imgWidth;

        System.out.println("Magic: " + magic);
        System.out.println("Num of images: " + numOfImages);
        System.out.println("Num of pixels: " + numOfPixels);

        double[][] vecs = new double[numOfImages][numOfPixels + 1];

        for (int imgNum = 0; imgNum < numOfImages; imgNum++) {
            vecs[imgNum][numOfPixels] = isLabels.read();
            for (int p = 0; p < numOfPixels; p++) {
                int c = 128 - isImages.read();
                vecs[imgNum][p] = (double)c / 128;
            }
        }

        List<double[]> lst = Arrays.asList(vecs);
        Collections.shuffle(lst, rnd);

        isImages.close();
        isLabels.close();

        return lst.subList(0, cnt).stream().map(DenseLocalOnHeapVector::new);
    }

    public static void asLIBSVM(String imgPath, String labelsPath, String outPath, Random rnd, int cnt) throws IOException {

        try (FileWriter fos = new FileWriter(outPath)) {
            mnist(imgPath, labelsPath, rnd, cnt).forEach(vec -> {
                try {
                    fos.write((int)vec.get(vec.size() - 1) + " ");

                    for (int i = 0; i < vec.size() - 1; i++) {
                        double val = vec.get(i);

                        if (val != 0)
                            fos.write((i + 1) + ":" + val + " ");
                    }

                    fos.write("\n");

                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static int read4Bytes(FileInputStream is) throws IOException {
        return (is.read() << 24) | (is.read() << 16) | (is.read() << 8) | (is.read());
    }
}
