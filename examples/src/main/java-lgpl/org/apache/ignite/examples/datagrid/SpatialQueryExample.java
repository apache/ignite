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

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import javax.cache.*;
import java.util.*;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

/**
 * This examples shows the usage of geospatial queries and indexes in Apache Ignite.
 * For more information please refer to the following technical documentation:
 * http://apacheignite.readme.io/docs/geospatial-queries
 * <p>
 * Remote nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class SpatialQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = SpatialQueryExample.class.getSimpleName();

    /**
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        // Starting Ignite node.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            // Preparing the cache configuration.
            CacheConfiguration<Integer, MapPoint> cc = new CacheConfiguration<>(CACHE_NAME);

            // Setting the indexed types.
            cc.setIndexedTypes(Integer.class, MapPoint.class);

            // Starting the cache.
            try (IgniteCache<Integer, MapPoint> cache = ignite.createCache(cc)) {
                Random rnd = new Random();

                WKTReader r = new WKTReader();

                // Adding geometry points into the cache.
                for (int i = 0; i < 1000; i++) {
                    int x = rnd.nextInt(10000);
                    int y = rnd.nextInt(10000);

                    Geometry geo = r.read("POINT(" + x + " " + y + ")");

                    cache.put(i, new MapPoint(geo));
                }

                // Query to fetch the points that fit into a specific polygon.
                SqlQuery<Integer, MapPoint> query = new SqlQuery<>(MapPoint.class, "coords && ?");

                // Selecting points that fit into a specific polygon.
                for (int i = 0; i < 10; i++) {
                    // Defining the next polygon boundaries.
                    Geometry cond = r.read("POLYGON((0 0, 0 " + rnd.nextInt(10000) + ", " +
                        rnd.nextInt(10000) + " " + rnd.nextInt(10000) + ", " +
                        rnd.nextInt(10000) + " 0, 0 0))");

                    // Executing the query.
                    Collection<Cache.Entry<Integer, MapPoint>> entries = cache.query(query.setArgs(cond)).getAll();

                    // Printing number of points that fit into the area defined by the polygon.
                    System.out.println("Fetched points [cond=" + cond + ", cnt=" + entries.size() + ']');
                }
            }
        }
    }

    /**
     * MapPoint with indexed coordinates.
     */
    private static class MapPoint {
        /** Coordinates. */
        @QuerySqlField(index = true)
        private Geometry coords;

        /**
         * @param coords Coordinates.
         */
        private MapPoint(Geometry coords) {
            this.coords = coords;
        }
    }
}