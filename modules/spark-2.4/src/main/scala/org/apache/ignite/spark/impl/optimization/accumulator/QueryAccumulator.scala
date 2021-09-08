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

package org.apache.ignite.spark.impl.optimization.accumulator

import org.apache.ignite.spark.impl.optimization.IgniteQueryContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Generic query info accumulator interface.
  */
private[apache] trait QueryAccumulator extends LogicalPlan {
    /**
      * @return Ignite query context.
      */
    def igniteQueryContext: IgniteQueryContext

    /**
      * @return Generated output.
      */
    def outputExpressions: Seq[NamedExpression]

    /**
      * @return Ordering info.
      */
    def orderBy: Option[Seq[SortOrder]]

    /**
      * @param outputExpressions New output expressions.
      * @return Copy of this accumulator with new output.
      */
    def withOutputExpressions(outputExpressions: Seq[NamedExpression]): QueryAccumulator

    /**
      * @param orderBy New ordering.
      * @return Copy of this accumulator with new order.
      */
    def withOrderBy(orderBy: Seq[SortOrder]): QueryAccumulator

    /**
      * @return Copy of this accumulator with `limit` expression.
      */
    def withLimit(limit: Expression): QueryAccumulator

    /**
      * @return Copy of this accumulator with `localLimit` expression.
      */
    def withLocalLimit(localLimit: Expression): QueryAccumulator

    /**
      * @param prettyPrint If true human readable query will be generated.
      * @return SQL query.
      */
    def compileQuery(prettyPrint: Boolean = false, nestedQuery: Boolean = false): String

    /**
      * @return Qualifier that should be use to select data from this accumulator.
      */
    def qualifier: String

    /**
      * All expressions are resolved when extra optimization executed.
      */
    override lazy val resolved = true
}
