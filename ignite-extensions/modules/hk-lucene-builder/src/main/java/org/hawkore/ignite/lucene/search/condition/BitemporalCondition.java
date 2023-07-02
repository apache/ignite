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

import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.schema.mapping.BitemporalMapper;
import org.hawkore.ignite.lucene.schema.mapping.BitemporalMapper.BitemporalDateTime;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches bi-temporal (four) fields within two range of values.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalCondition extends SingleMapperCondition<BitemporalMapper> {

    /** The default from value for vtFrom and ttFrom. */
    private static final Long DEFAULT_FROM = 0L;

    /** The default to value for vtTo and ttTo. */
    private static final Long DEFAULT_TO = Long.MAX_VALUE;

    /** The Valid Time Start. */
    final Object vtFrom;

    /** The Valid Time End. */
    final Object vtTo;

    /** The Transaction Time Start. */
    final Object ttFrom;

    /** The Transaction Time End. */
    final Object ttTo;

    /**
     * Constructs a query selecting all fields that intersects with valid time and transaction time ranges including
     * limits.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param vtFrom the valid time start
     * @param vtTo the valid time end
     * @param ttFrom the transaction time start
     * @param ttTo the transaction time end
     */
    public BitemporalCondition(Float boost, String field, Object vtFrom, Object vtTo, Object ttFrom, Object ttTo) {
        super(boost, field, BitemporalMapper.class);
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(BitemporalMapper mapper, Analyzer analyzer) {

        Long vtFromTime = parseTime(mapper, DEFAULT_FROM, vtFrom);
        Long vtToTime = parseTime(mapper, DEFAULT_TO, vtTo);
        Long ttFromTime = parseTime(mapper, DEFAULT_FROM, ttFrom);
        Long ttToTime = parseTime(mapper, DEFAULT_TO, ttTo);

        Long minTime = BitemporalDateTime.MIN.toTimestamp();
        Long maxTime = BitemporalDateTime.MAX.toTimestamp();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        if (!((vtFromTime.equals(0L)) && (vtToTime.equals(Long.MAX_VALUE)))) {

            BooleanQuery.Builder validBuilder = new BooleanQuery.Builder();
            validBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.VT_FROM_FIELD_SUFFIX,
                                          vtFromTime,
                                          vtToTime), SHOULD);
            validBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.VT_TO_FIELD_SUFFIX,
                                          vtFromTime,
                                          vtToTime), SHOULD);

            BooleanQuery.Builder containsValidBuilder = new BooleanQuery.Builder();
            containsValidBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.VT_FROM_FIELD_SUFFIX,
                                                  minTime,
                                                  vtFromTime), MUST);
            containsValidBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.VT_TO_FIELD_SUFFIX,
                                                  vtToTime,
                                                  maxTime), MUST);
            validBuilder.add(containsValidBuilder.build(), SHOULD);
            builder.add(validBuilder.build(), MUST);
        }

        if (!((ttFromTime.equals(0L)) && (ttToTime.equals(Long.MAX_VALUE)))) {

            BooleanQuery.Builder transactionBuilder = new BooleanQuery.Builder();
            transactionBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.TT_FROM_FIELD_SUFFIX,
                                                ttFromTime,
                                                ttToTime), SHOULD);
            transactionBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.TT_TO_FIELD_SUFFIX,
                                                ttFromTime,
                                                ttToTime), SHOULD);

            BooleanQuery.Builder containsTransactionBuilder = new BooleanQuery.Builder();
            containsTransactionBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.TT_FROM_FIELD_SUFFIX,
                                                        minTime,
                                                        ttFromTime), MUST);
            containsTransactionBuilder.add(LongPoint.newRangeQuery(field + BitemporalMapper.TT_TO_FIELD_SUFFIX,
                                                        ttToTime,
                                                        maxTime), MUST);
            transactionBuilder.add(containsTransactionBuilder.build(), SHOULD);
            builder.add(transactionBuilder.build(), MUST);
        }

        return builder.build();
    }

    private static Long parseTime(BitemporalMapper mapper, Long defaultTime, Object value) {
        return value == null
               ? new BitemporalDateTime(defaultTime).toTimestamp()
               : mapper.parseBitemporalDate(value).toTimestamp();
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("vtFrom", vtFrom)
                                   .add("vtTo", vtTo)
                                   .add("ttFrom", ttFrom)
                                   .add("ttTo", ttTo);
    }
}
