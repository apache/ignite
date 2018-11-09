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

package org.apache.ignite.examples.ml.tutorial;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Scanner;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * The utility class.
 */
public class TitanicUtils {
    /**
     * Read passengers data from csv file.
     *
     * @param ignite The ignite.
     * @return The filled cache.
     * @throws FileNotFoundException If data file is not found.
     */
    public static IgniteCache<Integer, Object[]> readPassengers(Ignite ignite)
        throws FileNotFoundException {
        IgniteCache<Integer, Object[]> cache = getCache(ignite);
        Scanner scanner = new Scanner(new File("examples/src/main/resources/datasets/titanic.csv"));

        int cnt = 0;
        while (scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if(cnt == 0) {
                cnt++;
                continue;
            }
            String[] cells = row.split(";");
            Object[] data = new Object[cells.length];
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
                        data[i] = cells[i];
                    }
                }
            cache.put(cnt++, data);
        }
        return cache;
    }

    /**
     * Fills cache with data and returns it.
     *
     * @param ignite Ignite instance.
     * @return Filled Ignite Cache.
     */
    private static IgniteCache<Integer, Object[]> getCache(Ignite ignite) {

        CacheConfiguration<Integer, Object[]> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TUTORIAL_" + UUID.randomUUID());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

        return ignite.createCache(cacheConfiguration);
    }
}
