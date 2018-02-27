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
    private final String common;

    /** HDFS home. */
    private final String hdfs;

    /** Mapred home. */
    private final String mapred;

    /** Whether common home exists. */
    private final boolean commonExists;

    /** Whether HDFS home exists. */
    private final boolean hdfsExists;

    /** Whether mapred home exists. */
    private final boolean mapredExists;

    /**
     * Constructor.
     *
     * @param home Hadoop home.
     * @param common Common home.
     * @param hdfs HDFS home.
     * @param mapred Mapred home.
     */
    public HadoopLocations(String home, String common, String hdfs, String mapred) {
        assert common != null && hdfs != null && mapred != null;

        this.home = home;
        this.common = common;
        this.hdfs = hdfs;
        this.mapred = mapred;

        commonExists = HadoopClasspathUtils.exists(common);
        hdfsExists = HadoopClasspathUtils.exists(hdfs);
        mapredExists = HadoopClasspathUtils.exists(mapred);
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
    public String common() {
        return common;
    }

    /**
     * @return HDFS home.
     */
    public String hdfs() {
        return hdfs;
    }

    /**
     * @return Mapred home.
     */
    public String mapred() {
        return mapred;
    }

    /**
     * @return Whether common home exists.
     */
    public boolean commonExists() {
        return commonExists;
    }

    /**
     * @return Whether HDFS home exists.
     */
    public boolean hdfsExists() {
        return hdfsExists;
    }

    /**
     * @return Whether mapred home exists.
     */
    public boolean mapredExists() {
        return mapredExists;
    }

    /**
     * Whether all required directories exists.
     *
     * @return {@code True} if exists.
     */
    public boolean valid() {
        return commonExists && hdfsExists && mapredExists;
    }
}