/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * Maintains query statistics.
 */
public class QueryStatisticsData {

    private static final Comparator<QueryEntry> QUERY_ENTRY_COMPARATOR =
            new Comparator<QueryEntry>() {
        @Override
        public int compare(QueryEntry o1, QueryEntry o2) {
            return (int) Math.signum(o1.lastUpdateTime - o2.lastUpdateTime);
        }
    };

    private final HashMap<String, QueryEntry> map =
            new HashMap<>();

    private int maxQueryEntries;

    public QueryStatisticsData(int maxQueryEntries) {
        this.maxQueryEntries = maxQueryEntries;
    }

    public synchronized void setMaxQueryEntries(int maxQueryEntries) {
        this.maxQueryEntries = maxQueryEntries;
    }

    public synchronized List<QueryEntry> getQueries() {
        // return a copy of the map so we don't have to
        // worry about external synchronization
        ArrayList<QueryEntry> list = new ArrayList<>(map.values());
        // only return the newest 100 entries
        Collections.sort(list, QUERY_ENTRY_COMPARATOR);
        return list.subList(0, Math.min(list.size(), maxQueryEntries));
    }

    /**
     * Update query statistics.
     *
     * @param sqlStatement the statement being executed
     * @param executionTimeNanos the time in nanoseconds the query/update took
     *            to execute
     * @param rowCount the query or update row count
     */
    public synchronized void update(String sqlStatement, long executionTimeNanos,
            int rowCount) {
        QueryEntry entry = map.get(sqlStatement);
        if (entry == null) {
            entry = new QueryEntry(sqlStatement);
            map.put(sqlStatement, entry);
        }
        entry.update(executionTimeNanos, rowCount);

        // Age-out the oldest entries if the map gets too big.
        // Test against 1.5 x max-size so we don't do this too often
        if (map.size() > maxQueryEntries * 1.5f) {
            // Sort the entries by age
            ArrayList<QueryEntry> list = new ArrayList<>(map.values());
            Collections.sort(list, QUERY_ENTRY_COMPARATOR);
            // Create a set of the oldest 1/3 of the entries
            HashSet<QueryEntry> oldestSet =
                    new HashSet<>(list.subList(0, list.size() / 3));
            // Loop over the map using the set and remove
            // the oldest 1/3 of the entries.
            for (Iterator<Entry<String, QueryEntry>> it =
                    map.entrySet().iterator(); it.hasNext();) {
                Entry<String, QueryEntry> mapEntry = it.next();
                if (oldestSet.contains(mapEntry.getValue())) {
                    it.remove();
                }
            }
        }
    }

    /**
     * The collected statistics for one query.
     */
    public static final class QueryEntry {

        /**
         * The SQL statement.
         */
        public final String sqlStatement;

        /**
         * The number of times the statement was executed.
         */
        public int count;

        /**
         * The last time the statistics for this entry were updated,
         * in milliseconds since 1970.
         */
        public long lastUpdateTime;

        /**
         * The minimum execution time, in nanoseconds.
         */
        public long executionTimeMinNanos;

        /**
         * The maximum execution time, in nanoseconds.
         */
        public long executionTimeMaxNanos;

        /**
         * The total execution time.
         */
        public long executionTimeCumulativeNanos;

        /**
         * The minimum number of rows.
         */
        public int rowCountMin;

        /**
         * The maximum number of rows.
         */
        public int rowCountMax;

        /**
         * The total number of rows.
         */
        public long rowCountCumulative;

        /**
         * The mean execution time.
         */
        public double executionTimeMeanNanos;

        /**
         * The mean number of rows.
         */
        public double rowCountMean;

        // Using Welford's method, see also
        // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        // http://www.johndcook.com/standard_deviation.html

        private double executionTimeM2Nanos;
        private double rowCountM2;

        public QueryEntry(String sql) {
            this.sqlStatement = sql;
        }

        /**
         * Update the statistics entry.
         *
         * @param timeNanos the execution time in nanos
         * @param rows the number of rows
         */
        void update(long timeNanos, int rows) {
            count++;
            executionTimeMinNanos = Math.min(timeNanos, executionTimeMinNanos);
            executionTimeMaxNanos = Math.max(timeNanos, executionTimeMaxNanos);
            rowCountMin = Math.min(rows, rowCountMin);
            rowCountMax = Math.max(rows, rowCountMax);

            double rowDelta = rows - rowCountMean;
            rowCountMean += rowDelta / count;
            rowCountM2 += rowDelta * (rows - rowCountMean);

            double timeDelta = timeNanos - executionTimeMeanNanos;
            executionTimeMeanNanos += timeDelta / count;
            executionTimeM2Nanos += timeDelta * (timeNanos - executionTimeMeanNanos);

            executionTimeCumulativeNanos += timeNanos;
            rowCountCumulative += rows;
            lastUpdateTime = System.currentTimeMillis();
        }

        public double getExecutionTimeStandardDeviation() {
            // population standard deviation
            return Math.sqrt(executionTimeM2Nanos / count);
        }

        public double getRowCountStandardDeviation() {
            // population standard deviation
            return Math.sqrt(rowCountM2 / count);
        }

    }

}
