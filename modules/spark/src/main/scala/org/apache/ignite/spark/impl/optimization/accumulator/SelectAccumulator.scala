/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark.impl.optimization.accumulator

import org.apache.spark.sql.catalyst.expressions.Expression

/**
  * Generic interface for a SELECT query.
  */
private[apache] trait SelectAccumulator extends QueryAccumulator {
    /**
      * @return Expression for HAVING part of query.
      */
    def having: Option[Seq[Expression]]

    /**
      * @return Expression for WHERE part of query.
      */
    def where: Option[Seq[Expression]]

    /**
      * @return Expression for GROUP BY part of query.
      */
    def groupBy: Option[Seq[Expression]]

    /**
      * @return Copy of this accumulator with `distinct` flag.
      */
    def withDistinct(distinct: Boolean): SelectAccumulator

    /**
      * @return Copy of this accumulator with `where` expressions.
      */
    def withWhere(where: Seq[Expression]): SelectAccumulator

    /**
      * @return Copy of this accumulator with `groupBy` expressions.
      */
    def withGroupBy(groupBy: Seq[Expression]): SelectAccumulator

    /**
      * @return Copy of this accumulator with `having` expressions.
      */
    def withHaving(having: Seq[Expression]): SelectAccumulator

    /**
      * @return Copy of this accumulator with `limit` expression.
      */
    def withLimit(limit: Expression): SelectAccumulator

    /**
      * @return Copy of this accumulator with `localLimit` expression.
      */
    def withLocalLimit(localLimit: Expression): SelectAccumulator
}
