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

package org.apache.ignite.jdbc.thin;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public class JdbcThinMissingLongArrayResultsTest extends JdbcThinAbstractSelfTest {
    /** First cache name. */
    private static final String CACHE_NAME = "test";

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Rand. */
    private static final Random RAND = new Random(123);

    /** Sample size. */
    private static final int SAMPLE_SIZE = 1_000_000;

    /** Block size. */
    private static final int BLOCK_SIZE = 5_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setIndexedTypes(Key.class, Value.class);

        cfg.setName(name);

//        cfg.setSqlSchema(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);

        Ignite ignite = ignite(0);

        try (IgniteDataStreamer<Key, Value> streamer = ignite.dataStreamer(CACHE_NAME)) {
            streamer.allowOverwrite(true);
            streamer.perNodeBufferSize(50);
            streamer.autoFlushFrequency(TimeUnit.SECONDS.toMillis(45));
            streamer.skipStore(false);
            streamer.keepBinary(true);

            int secId = 0;

            final int maxBlocks = SAMPLE_SIZE / BLOCK_SIZE;

            Key key = new Key();
            Value value = new Value();
            long date = System.nanoTime();

            for (int blockId = 0; blockId < maxBlocks; blockId++) {

                final long[] time = new long[BLOCK_SIZE];
                final double[] open = new double[BLOCK_SIZE];
                final double[] close = new double[BLOCK_SIZE];
                final double[] high = new double[BLOCK_SIZE];
                final double[] low = new double[BLOCK_SIZE];
                final double[] marketVWAP = new double[BLOCK_SIZE];

                for (int i = 0; i < BLOCK_SIZE; i++) {
                    // Fake data
                    time[i] = System.nanoTime();
                    open[i] = Math.abs(RAND.nextGaussian());
                    close[i] = Math.abs(RAND.nextGaussian());
                    high[i] = Math.max(open[i], Math.abs(RAND.nextGaussian()));
                    low[i] = Math.min(open[i], Math.abs(RAND.nextGaussian()));
                    marketVWAP[i] = Math.abs(RAND.nextGaussian());
                }

                // Add to the cache
                key.setDate(date);
                key.setSecurityId(secId);

                value.setTime(time); // BUG  in providing it via JDBC

                value.setOpen(open);
                value.setHigh(high);
                value.setLow(low);
                value.setClose(close);
                value.setMarketVWAP(marketVWAP);

                streamer.addData(key, value);

                secId++; // for unique values
                //secId = RAND.nextInt(1000);

                if (blockId % 100 == 0)
                    System.out.println("+++ Processed " + (blockId * BLOCK_SIZE) + " events so far " + new Date());
            }
            streamer.flush();
        }
        ignite.active(true);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unused"})
    @Test
    public void testDefaults() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema('"' + CACHE_NAME + '"');

            try (PreparedStatement st = conn.prepareStatement("SELECT * FROM VALUE")) {
                ResultSet rs = st.executeQuery();

                int cols = rs.getMetaData().getColumnCount();

                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();

                    sb.append(rs.getObject(1).toString());

                    for (int i = 1; i < cols; ++i)
                        sb.append(", ").append(rs.getObject(i + 1).toString());

                    System.out.println(sb.toString());
                }
            }
        }
    }

    /**
     *
     */
    @SuppressWarnings("unused")
    public static class Key implements Serializable {
        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(
            name = "date_sec_idx", order = 0, descending = true)})
        private long date;

        @QuerySqlField(index = true, orderedGroups = {@QuerySqlField.Group(
            name = "date_sec_idx", order = 3)})
        private int securityId;

        /**
         *
         */
        public Key() {
            // Required for default binary serialization
        }

        /**
         * @return Date.
         */
        public long getDate() {
            return date;
        }

        /**
         * @param securityId Security ID.
         */
        public void setSecurityId(int securityId) {
            this.securityId = securityId;
        }

        /**
         * @param date Date.
         */
        public void setDate(long date) {
            this.date = date;
        }
    }

    /**
     *
     */
    public static class Value implements Serializable {

        /** Time. */
        @QuerySqlField()
        private long[] time;

        /** Open. */
        @QuerySqlField()
        private double[] open;

        /** High. */
        @QuerySqlField()
        private double[] high;

        /** Low. */
        @QuerySqlField()
        private double[] low;

        /** Close. */
        @QuerySqlField()
        private double[] close;

        /** Market vwap. */
        @QuerySqlField()
        private double[] marketVWAP;

        /**
         *
         */
        public Value() {
            // Required for default binary serialization
        }

        /**
         * @return close.
         */
        public double[] getClose() {
            return close;
        }

        /**
         * @return time.
         */
        public long[] getTime() {
            return time;
        }

        /**
         * @param time time.
         */
        public void setTime(long[] time) {
            this.time = time;
        }

        /**
         * @param open open.
         */
        public void setOpen(double[] open) {
            this.open = open;
        }

        /**
         * @param high high.
         */
        public void setHigh(double[] high) {
            this.high = high;
        }

        /**
         * @param low low.
         */
        public void setLow(double[] low) {
            this.low = low;
        }

        /**
         * @param close close.
         */
        public void setClose(double[] close) {
            this.close = close;
        }

        /**
         * @param marketVWAP marketVWAP.
         */
        public void setMarketVWAP(double[] marketVWAP) {
            this.marketVWAP = marketVWAP;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "OHLC{" +
                ", time=" + Arrays.toString(time) +
                ", open=" + Arrays.toString(open) +
                ", high=" + Arrays.toString(high) +
                ", low=" + Arrays.toString(low) +
                ", close=" + Arrays.toString(close) +
                ", marketVWAP=" + Arrays.toString(marketVWAP) +
                '}';
        }
    }
}
