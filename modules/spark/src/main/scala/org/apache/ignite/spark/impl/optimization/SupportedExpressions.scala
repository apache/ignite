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

package org.apache.ignite.spark.impl.optimization

import org.apache.spark.sql.catalyst.expressions.Expression

/**
  * Provides methods to work with Spark SQL expression that supported by Ignite SQL syntax.
  */
private[optimization] trait SupportedExpressions {
    /**
      * @param expr Expression to check.
      * @param checkChild Closure to check child expression.
      * @return True if `expr` are supported, false otherwise.
      */
    def apply(expr: Expression, checkChild: (Expression) ⇒ Boolean): Boolean

    /**
      * @param expr Expression to convert to string.
      * @param childToString Closure to convert children expressions.
      * @param useQualifier If true `expr` should be printed using qualifier. `Table1.id` for example.
      * @param useAlias If true `expr` should be printed with alias. `name as person_name` for example.
      * @return SQL representation of `expr` if it supported. `None` otherwise.
      */
    def toString(expr: Expression, childToString: (Expression) ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String]
}
