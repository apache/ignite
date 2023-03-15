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
package org.hawkore.ignite.lucene.search.condition;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.DocValuesNumbersQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RangeCondition extends SingleColumnCondition {

    /** The default include lower option. */
    public static final boolean DEFAULT_INCLUDE_LOWER = false;

    /** The default include upper option. */
    public static final boolean DEFAULT_INCLUDE_UPPER = false;

    /** The default use doc values option. */
    public static final boolean DEFAULT_DOC_VALUES = false;

    /** The lower accepted value. Maybe null meaning no lower limit. */
    public final Object lower;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    public final Object upper;

    /** If the lower value must be included if not null. */
    public final boolean includeLower;

    /** If the upper value must be included if not null. */
    public final boolean includeUpper;

    /** If the generated query should use doc values. */
    public final boolean docValues;

    /**
     * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal than {@code
     * upperValue}.
     *
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched.
     * @param lowerValue the lower accepted value. Maybe {@code null} meaning no lower limit
     * @param upperValue the upper accepted value. Maybe {@code null} meaning no upper limit
     * @param includeLower if {@code true}, the {@code lowerValue} is included in the range
     * @param includeUpper if {@code true}, the {@code upperValue} is included in the range
     * @param docValues if the generated query should use doc values
     */
    public RangeCondition(Float boost,
                          String field,
                          Object lowerValue,
                          Object upperValue,
                          Boolean includeLower,
                          Boolean includeUpper,
                          Boolean docValues) {
        super(boost, field);
        this.lower = lowerValue;
        this.upper = upperValue;
        this.includeLower = includeLower == null ? DEFAULT_INCLUDE_LOWER : includeLower;
        this.includeUpper = includeUpper == null ? DEFAULT_INCLUDE_UPPER : includeUpper;
        this.docValues = docValues == null ? DEFAULT_DOC_VALUES : docValues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {

        // Check doc values
        if (docValues && !mapper.docValues) {
            throw new IndexException("Field '{}' does not support doc_values", mapper.field);
        }

        Class<?> clazz = mapper.base;
        Query query;
        if (clazz == String.class) {
            String start = (String) mapper.base(field, lower);
            String stop = (String) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Integer.class) {
            Integer start = (Integer) mapper.base(field, lower);
            Integer stop = (Integer) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Long.class) {
            Long start = (Long) mapper.base(field, lower);
            Long stop = (Long) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Float.class) {
            Float start = (Float) mapper.base(field, lower);
            Float stop = (Float) mapper.base(field, upper);
            query = query(start, stop);
        } else if (clazz == Double.class) {
            Double start = (Double) mapper.base(field, lower);
            Double stop = (Double) mapper.base(field, upper);
            query = query(start, stop);
        } else {
            throw new IndexException("Range queries are not supported by mapper '{}'", mapper);
        }
        return query;
    }

    private Query query(String start, String stop) {
        return docValues
               ? new DocValuesTermsQuery(field, docValue(start),
                                                      docValue(stop))
               : TermRangeQuery.newStringRange(field, start, stop, includeLower, includeUpper);
    }


    private Query query(Integer start, Integer stop) {

        Query q = null;

        Integer lower = start == null ? Integer.MIN_VALUE: start;
        Integer upper = stop == null ? Integer.MAX_VALUE: stop;

        if(!includeLower){
            lower = Math.addExact(lower, 1);
        }

        if(!includeUpper){
            upper = Math.addExact(upper, -1);
        }

        if (docValues){
            q = new DocValuesNumbersQuery(field, docValue(lower), docValue(upper));
        }else{
            q = IntPoint.newRangeQuery(field, lower, upper);
        }

        return q;
    }

    private Query query(Long start, Long stop) {

        Query q = null;

        Long lower = start == null ? Long.MIN_VALUE: start;
        Long upper = stop == null ? Long.MAX_VALUE: stop;

        if(!includeLower){
            lower = Math.addExact(lower, 1);
        }

        if(!includeUpper){
            upper = Math.addExact(upper, -1);
        }

        if (docValues){
            q = new DocValuesNumbersQuery(field, lower, upper);
        }else{
            q = LongPoint.newRangeQuery(field, lower, upper);
        }

        return q;
    }

    private Query query(Float start, Float stop) {
        Query q = null;

        Float lower = start == null ? Float.NEGATIVE_INFINITY: start;
        Float upper = stop == null ? Float.POSITIVE_INFINITY: stop;

        if(!includeLower){
            lower = FloatPoint.nextUp(lower);
        }

        if(!includeUpper){
            upper = FloatPoint.nextDown(upper);
        }

        if (docValues){
            q = new DocValuesNumbersQuery(field, docValue(lower), docValue(upper));
        }else{
            q = FloatPoint.newRangeQuery(field, lower, upper);
        }

        return q;
    }

    private Query query(Double start, Double stop) {
        Query q = null;

        Double lower = start == null ? Double.NEGATIVE_INFINITY: start;
        Double upper = stop == null ? Double.POSITIVE_INFINITY: stop;

        if(!includeLower){
            lower = DoublePoint.nextUp(lower);
        }

        if(!includeUpper){
            upper = DoublePoint.nextDown(upper);
        }

        if (docValues){
            q = new DocValuesNumbersQuery(field, docValue(lower), docValue(upper));
        }else{
            q = DoublePoint.newRangeQuery(field, lower, upper);
        }

        return q;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("lower", lower)
                                   .add("upper", upper)
                                   .add("includeLower", includeLower)
                                   .add("includeUpper", includeUpper)
                                   .add("docValues", docValues);
    }
}
