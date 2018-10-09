package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Service helper to handle collecting of metrics.
 */
public interface GridCacheMetricsCollector {
    /**
     * End point of collecting metrics.
     * @return This instance for chain calls.
     */
    GridCacheMetricsCollector finish();

    /**
     * Main call for metrics creation.
     * @param qryType Query type to save in metric.
     * @param qry Query text to save in metric.
     * @param qryMgr Query manager to delegate creation of metrics.
     * @param e Error to save in metric.
     */
    void collectMetrics(GridCacheQueryType qryType, String qry, GridCacheQueryManager qryMgr, Throwable e);

    /**
     *
     * @return Time in ms when collecting of metrics was started .
     */
    long startTime();

    /**
     *
     * @return Difference between finish time and start time in ms.
     */
    long duration();

    /**
     * Set flag enables call to query manager.
     * (is set by default, used for special cases where custom management required.
     * @return This instance for chain calls.
     */
    GridCacheMetricsCollector arm();

    class Default implements GridCacheMetricsCollector {
        /** */
        private volatile long startTime;

        /** */
        private volatile long finishTime;

        /** */
        private volatile boolean armed;

        /** */
        private AtomicBoolean finished = new AtomicBoolean(false);

        /**
         *
         * @return Instance with enabled management of call to query manager.
         */
        public static GridCacheMetricsCollector createWithTrigger(){
            return new Default(true);
        }

        /**
         *
         * @return Instance without management of call to query manager.
         */
        public static GridCacheMetricsCollector create(){
            return new Default(false);
        }

        /**
         *
         * @param wTrigger With or without management of call to query manager.
         */
        public Default(boolean wTrigger){
            armed = !wTrigger;
            startTime = U.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public GridCacheMetricsCollector finish() {
            if (finished.compareAndSet(false, true))
                finishTime = U.currentTimeMillis();

            return this;
        }

        /** {@inheritDoc} */
        @Override public void collectMetrics(
            final GridCacheQueryType qryType,
            final String qry,
            final GridCacheQueryManager qryMgr,
            final Throwable e) {

            GridCacheQueryManager qryMgr0 = qryMgr;

            if (!finished.get())
                throw new IgniteException("Unable to collect metrics with metrics collector that is not finished.");

            if (armed)
                qryMgr0.collectMetrics(qryType, qry, startTime(), duration(), e != null);
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            return startTime;
        }

        /** {@inheritDoc} */
        @Override public long duration() {
            return finishTime - startTime;
        }

        /** {@inheritDoc} */
        @Override public GridCacheMetricsCollector arm() {
            armed = true;

            return this;
        }
    }
}
