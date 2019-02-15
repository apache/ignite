/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
