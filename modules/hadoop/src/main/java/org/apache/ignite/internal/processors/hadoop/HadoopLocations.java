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

package org.apache.ignite.internal.processors.hadoop;

/**
 * Simple structure to hold Hadoop directory locations.
 */
public class HadoopLocations {
    /** Hadoop home. */
    private final String home;

    /** Common home. */
    private final String commonHome;

    /** HDFS home. */
    private final String hdfsHome;

    /** Mapred home. */
    private final String mapredHome;

    /**
     * Constructor.
     *
     * @param home Hadoop home.
     * @param commonHome Common home.
     * @param hdfsHome HDFS home.
     * @param mapredHome Mapred home.
     */
    public HadoopLocations(String home, String commonHome, String hdfsHome, String mapredHome) {
        this.home = home;
        this.commonHome = commonHome;
        this.hdfsHome = hdfsHome;
        this.mapredHome = mapredHome;
    }

    /**
     * @return Hadoop home.
     */
    public String home() {
        return home;
    }

    /**
     * @return Common home.
     */
    public String commonHome() {
        return commonHome;
    }

    /**
     * @return HDFS home.
     */
    public String hdfsHome() {
        return hdfsHome;
    }

    /**
     * @return Mapred home.
     */
    public String mapredHome() {
        return mapredHome;
    }
}
