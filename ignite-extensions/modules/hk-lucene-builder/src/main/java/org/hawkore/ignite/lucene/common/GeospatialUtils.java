/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.common;

import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.hawkore.ignite.lucene.IndexException;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;

/**
 * Utilities for geospatial related stuff.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeospatialUtils {

    /** The spatial context to be used. */
    public static final SpatialContext CONTEXT = SpatialContext.GEO;

    /** The min accepted longitude. */
    public static final double MIN_LATITUDE = -90.0;

    /** The max accepted longitude. */
    public static final double MAX_LATITUDE = 90.0;

    /** The min accepted longitude. */
    public static final double MIN_LONGITUDE = -180.0;

    /** The max accepted longitude. */
    public static final double MAX_LONGITUDE = 180.0;

    /**
     * Checks if the specified max levels is correct.
     *
     * @param userMaxLevels the maximum number of levels in the tree
     * @param defaultMaxLevels the default max number of levels
     * @return the validated max levels
     */
    public static int validateGeohashMaxLevels(Integer userMaxLevels, int defaultMaxLevels) {
        int maxLevels = userMaxLevels == null ? defaultMaxLevels : userMaxLevels;
        if (maxLevels < 1 || maxLevels > GeohashPrefixTree.getMaxLevelsPossible()) {
            throw new IndexException("max_levels must be in range [1, {}], but found {}",
                                     GeohashPrefixTree.getMaxLevelsPossible(),
                                     maxLevels);
        }
        return maxLevels;
    }

    /**
     * Checks if the specified latitude is correct.
     *
     * @param name the name of the latitude field
     * @param latitude the value of the latitude field
     * @return the latitude
     */
    public static Double checkLatitude(String name, Double latitude) {
        if (latitude == null) {
            throw new IndexException("{} required", name);
        } else if (latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
            throw new IndexException("{} must be in range [{}, {}], but found {}",
                                     name,
                                     MIN_LATITUDE,
                                     MAX_LATITUDE,
                                     latitude);
        }
        return latitude;
    }

    /**
     * Checks if the specified longitude is correct.
     *
     * @param name the name of the longitude field
     * @param longitude the value of the longitude field
     * @return the longitude
     */
    public static Double checkLongitude(String name, Double longitude) {
        if (longitude == null) {
            throw new IndexException("{} required", name);
        } else if (longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
            throw new IndexException("{} must be in range [{}, {}], but found {}",
                                     name,
                                     MIN_LONGITUDE,
                                     MAX_LONGITUDE,
                                     longitude);
        }
        return longitude;
    }
}
