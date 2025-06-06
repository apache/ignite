

package org.apache.ignite.console.dto;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;


/**
 * Queries notebooks.
 */
public class Notebook extends AbstractDto {
    /** Name. */
    @NotNull
    @NotEmpty
    private String name;

    /** Paragraphs. */
    private Paragraph[] paragraphs = {};

    /** Expanded paragraphs. */
    private int[] expandedParagraphs;
    
    /** default clusterId */
    private String clusterId;

    public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	/**
     * @return Name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name New name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Paragraphs.
     */
    public Paragraph[] getParagraphs() {
        return paragraphs;
    }

    /**
     * @param paragraphs New paragraphs.
     */
    public void setParagraphs(Paragraph[] paragraphs) {
        this.paragraphs = paragraphs;
    }

    /**
     * @return Expanded paragraphs.
     */
    public int[] getExpandedParagraphs() {
        return expandedParagraphs;
    }

    /**
     * @param expandedParagraphs New expanded paragraphs.
     */
    public void setExpandedParagraphs(int[] expandedParagraphs) {
        this.expandedParagraphs = expandedParagraphs;
    }

    /**
     * Paragraph in notebook.
     */
    public static class Paragraph {
        /** Name. */
        @NotNull
        @NotEmpty
        private String name;

        /** Query type. */
        @NotNull
        private QueryType qryType = QueryType.SQL_FIELDS;

        /** Query. */
        @NotNull
        @NotEmpty
        private String qry;

        /** Result presentation type. */
        private ResultType res = ResultType.NONE;

        /** Page size. */
        private int pageSize;

        /** Timeline span. */
        private int timeLineSpan;

        /** Max pages. */
        private int maxPages;

        /** Selected cache name. */
        private String cacheName;

        /** Use selected cache as default schema. */
        private boolean useAsDfltSchema;

        /** Non collocated joins. */
        private boolean nonCollocatedJoins;

        /** Enforce join order. */
        private boolean enforceJoinOrder;

        /** Lazy result set. */
        private boolean lazy;

        /** Collocated query. */
        private boolean collocated;

        /** Charts options. */
        private ChartOptions chartsOptions;

        /** Refresh settings. */
        private Rate rate;

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name New name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return Query type.
         */
        public QueryType getQueryType() {
            return qryType;
        }

        /**
         * @param qryType New query type.
         */
        public void setQueryType(QueryType qryType) {
            this.qryType = qryType;
        }

        /**
         * @return Query.
         */
        public String getQuery() {
            return qry;
        }

        /**
         * @param qry New query.
         */
        public void setQuery(String qry) {
            this.qry = qry;
        }

        /**
         * @return Result presentation type.
         */
        public ResultType getResult() {
            return res;
        }

        /**
         * @param res New result presentation type.
         */
        public void setResult(ResultType res) {
            this.res = res;
        }

        /**
         * @return Page size.
         */
        public int getPageSize() {
            return pageSize;
        }

        /**
         * @param pageSize New page size.
         */
        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        /**
         * @return Time line span.
         */
        public int getTimeLineSpan() {
            return timeLineSpan;
        }

        /**
         * @param timeLineSpan New time line span.
         */
        public void setTimeLineSpan(int timeLineSpan) {
            this.timeLineSpan = timeLineSpan;
        }

        /**
         * @return Max pages.
         */
        public int getMaxPages() {
            return maxPages;
        }

        /**
         * @param maxPages New max pages.
         */
        public void setMaxPages(int maxPages) {
            this.maxPages = maxPages;
        }

        /**
         * @return Selected cache name.
         */
        public String getCacheName() {
            return cacheName;
        }

        /**
         * @param cacheName New selected cache name.
         */
        public void setCacheName(String cacheName) {
            this.cacheName = cacheName;
        }

        /**
         * @return Use selected cache as default schema.
         */
        public boolean getUseAsDefaultSchema() {
            return useAsDfltSchema;
        }

        /**
         * @param useAsDfltSchema New use selected cache as default schema.
         */
        public void setUseAsDefaultSchema(boolean useAsDfltSchema) {
            this.useAsDfltSchema = useAsDfltSchema;
        }

        /**
         * @return Non collocated joins.
         */
        public boolean getNonCollocatedJoins() {
            return nonCollocatedJoins;
        }

        /**
         * @param nonCollocatedJoins New non collocated joins.
         */
        public void setNonCollocatedJoins(boolean nonCollocatedJoins) {
            this.nonCollocatedJoins = nonCollocatedJoins;
        }

        /**
         * @return Enforce join order.
         */
        public boolean getEnforceJoinOrder() {
            return enforceJoinOrder;
        }

        /**
         * @param enforceJoinOrder New enforce join order.
         */
        public void setEnforceJoinOrder(boolean enforceJoinOrder) {
            this.enforceJoinOrder = enforceJoinOrder;
        }

        /**
         * @return Lazy result set.
         */
        public boolean getLazy() {
            return lazy;
        }

        /**
         * @param lazy New lazy result set.
         */
        public void setLazy(boolean lazy) {
            this.lazy = lazy;
        }

        /**
         * @return Collocated query.
         */
        public boolean getCollocated() {
            return collocated;
        }

        /**
         * @param collocated New collocated query.
         */
        public void setCollocated(boolean collocated) {
            this.collocated = collocated;
        }

        /**
         * @return Charts options.
         */
        public ChartOptions getChartsOptions() {
            return chartsOptions;
        }

        /**
         * @param chartsOptions New charts options.
         */
        public void setChartsOptions(ChartOptions chartsOptions) {
            this.chartsOptions = chartsOptions;
        }

        /**
         * @return Refresh settings.
         */
        public Rate getRate() {
            return rate;
        }

        /**
         * @param rate New refresh settings.
         */
        public void setRate(Rate rate) {
            this.rate = rate;
        }
    }

    /**
     * Query type.
     */
    public enum QueryType {
        /** */
        SQL_FIELDS,

        /** */
        SCAN,
        
        /** graph */
        GREMLIN
    }

    /**
     *  Result presentation type.
     */
    public enum ResultType {
        /** None. */
        NONE,

        /** Table. */
        TABLE,

        /** Bar chart. */
        BAR,

        /** Pie chart. */
        PIE,

        /** Line chart. */
        LINE,

        /** Area chart. */
        AREA
    }

    /**
     * Chart settings.
     */
    public static class ChartOptions {
        /** Is Bar chart stacked. */
        private boolean barChartStacked = true;

        /** Area chart style. */
        private String areaChartStyle = "stack";

        /**
         * @return Bar chart stacked flag.
         */
        public boolean getBarChartStacked() {
            return barChartStacked;
        }

        /**
         * @param barChartStacked Bar chart stacked flag.
         */
        public void setBarChartStacked(boolean barChartStacked) {
            this.barChartStacked = barChartStacked;
        }

        /**
         * @return Area chart style.
         */
        public String getAreaChartStyle() {
            return areaChartStyle;
        }

        /**
         * @param areaChartStyle New area chart style.
         */
        public void setAreaChartStyle(String areaChartStyle) {
            this.areaChartStyle = areaChartStyle;
        }
    }

    /**
     * Refresh descriptor.
     */
    public static class Rate {
        /** Scale. */
        private int unit;

        /** Value. */
        private int val;

        /** Is refresh working. */
        private boolean installed;

        /**
         * @return Refresh time units.
         */
        public int getUnit() {
            return unit;
        }

        /**
         * @param unit Refresh time units.
         */
        public void setUnit(int unit) {
            this.unit = unit;
        }

        /**
         * @return Amount of time units.
         */
        public int getValue() {
            return val;
        }

        /**
         * @param val  Amount of time units.
         */
        public void setValue(int val) {
            this.val = val;
        }

        /**
         * @return {@code true} if refresh in progress.
         */
        public boolean isInstalled() {
            return installed;
        }

        /**
         * @param installed Refresh in progress flag.
         */
        public void setInstalled(boolean installed) {
            this.installed = installed;
        }
    }
}
