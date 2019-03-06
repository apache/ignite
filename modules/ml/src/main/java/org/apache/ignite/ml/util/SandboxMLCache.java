/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Scanner;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.exceptions.knn.FileParsingException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;


/**
 * Common utility code used in some ML examples to set up test cache.
 */
public class SandboxMLCache {
    /** */
    private final Ignite ignite;

    /** */
    public SandboxMLCache(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Fills cache with data and returns it.
     *
     * @param data Data to fill the cache with.
     * @return Filled Ignite Cache.
     */
    public IgniteCache<Integer, double[]> fillCacheWith(double[][] data) {
        CacheConfiguration<Integer, double[]> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TEST_" + UUID.randomUUID());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

        IgniteCache<Integer, double[]> cache = ignite.createCache(cacheConfiguration);

        for (int i = 0; i < data.length; i++)
            cache.put(i, data[i]);

        return cache;
    }

    /**
     * Fills cache with data and returns it.
     *
     * @param dataset The chosen dataset.
     * @return Filled Ignite Cache.
     * @throws FileNotFoundException If file not found.
     */
    public IgniteCache<Integer, Vector> fillCacheWith(MLSandboxDatasets dataset) throws FileNotFoundException {

        IgniteCache<Integer, Vector> cache = getCache();

        String fileName = dataset.getFileName();

        File file = IgniteUtils.resolveIgnitePath(fileName);

        if (file == null)
            throw new FileNotFoundException(fileName);

        Scanner scanner = new Scanner(file);

        int cnt = 0;
        while (scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if(dataset.hasHeader() && cnt == 0) {
                cnt++;
                continue;
            }

            String[] cells = row.split(dataset.getSeparator());

            double[] data = new double[cells.length];
            NumberFormat format = NumberFormat.getInstance(Locale.FRANCE);

            for (int i = 0; i < cells.length; i++)
                try{
                    if(cells[i].equals("")) data[i] = Double.NaN;
                    else data[i] = Double.valueOf(cells[i]);
                } catch (java.lang.NumberFormatException e) {
                    try {
                        data[i] = format.parse(cells[i]).doubleValue();
                    }
                    catch (ParseException e1) {
                        throw new FileParsingException(cells[i], i, Paths.get(dataset.getFileName()));
                    }
                }
            cache.put(cnt++, VectorUtils.of(data));
        }
        return cache;

    }

    /**
     * Fills cache with data and returns it.
     *
     * @return Filled Ignite Cache.
     */
    private IgniteCache<Integer, Vector> getCache() {

        CacheConfiguration<Integer, Vector> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TUTORIAL_" + UUID.randomUUID());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

        return ignite.createCache(cacheConfiguration);
    }
}
