/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

/**
 * The type of an aggregate function.
 */
public enum AggregateType {

    /**
     * The aggregate type for COUNT(*).
     */
    COUNT_ALL,

    /**
     * The aggregate type for COUNT(expression).
     */
    COUNT,

    /**
     * The aggregate type for SUM(expression).
     */
    SUM,

    /**
     * The aggregate type for MIN(expression).
     */
    MIN,

    /**
     * The aggregate type for MAX(expression).
     */
    MAX,

    /**
     * The aggregate type for AVG(expression).
     */
    AVG,

    /**
     * The aggregate type for STDDEV_POP(expression).
     */
    STDDEV_POP,

    /**
     * The aggregate type for STDDEV_SAMP(expression).
     */
    STDDEV_SAMP,

    /**
     * The aggregate type for VAR_POP(expression).
     */
    VAR_POP,

    /**
     * The aggregate type for VAR_SAMP(expression).
     */
    VAR_SAMP,

    /**
     * The aggregate type for ANY(expression).
     */
    ANY,

    /**
     * The aggregate type for EVERY(expression).
     */
    EVERY,

    /**
     * The aggregate type for BOOL_OR(expression).
     */
    BIT_OR,

    /**
     * The aggregate type for BOOL_AND(expression).
     */
    BIT_AND,

    /**
     * The aggregate type for SELECTIVITY(expression).
     */
    SELECTIVITY,

    /**
     * The aggregate type for HISTOGRAM(expression).
     */
    HISTOGRAM,

    /**
     * The type for RANK() hypothetical set function.
     */
    RANK,

    /**
     * The type for DENSE_RANK() hypothetical set function.
     */
    DENSE_RANK,

    /**
     * The type for PERCENT_RANK() hypothetical set function.
     */
    PERCENT_RANK,

    /**
     * The type for CUME_DIST() hypothetical set function.
     */
    CUME_DIST,

    /**
     * The aggregate type for PERCENTILE_CONT(expression).
     */
    PERCENTILE_CONT,

    /**
     * The aggregate type for PERCENTILE_DISC(expression).
     */
    PERCENTILE_DISC,

    /**
     * The aggregate type for MEDIAN(expression).
     */
    MEDIAN,

    /**
     * The aggregate type for LISTAGG(...).
     */
    LISTAGG,

    /**
     * The aggregate type for ARRAY_AGG(expression).
     */
    ARRAY_AGG,

    /**
     * The aggregate type for MODE(expression).
     */
    MODE,

    /**
     * The aggregate type for ENVELOPE(expression).
     */
    ENVELOPE,

}
