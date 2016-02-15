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

namespace Apache.Ignite.Linq.Impl
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using Remotion.Linq.Clauses;

    /// <summary>
    /// Alias dictionary.
    /// </summary>
    internal class AliasDictionary
    {
        /** */
        private int _aliasIndex;

        /** */
        private Dictionary<string, string> _aliases = new Dictionary<string, string>();

        /** */
        private readonly Stack<Dictionary<string, string>> _stack = new Stack<Dictionary<string, string>>();

        /// <summary>
        /// Pushes current aliases to stack.
        /// </summary>
        public void Push()
        {
            _stack.Push(_aliases);

            _aliases = new Dictionary<string, string>();
        }

        /// <summary>
        /// Pops current aliases from stack.
        /// </summary>
        public void Pop()
        {
            _aliases = _stack.Pop();
        }

        /// <summary>
        /// Gets the table alias.
        /// </summary>
        public string GetTableAlias(ICacheQueryableInternal queryable)
        {
            Debug.Assert(queryable != null);

            return GetTableAlias(ExpressionWalker.GetTableNameWithSchema(queryable));
        }

        /// <summary>
        /// Gets the table alias.
        /// </summary>
        public string GetTableAlias(string fullName)
        {
            Debug.Assert(!string.IsNullOrEmpty(fullName));

            string alias;

            if (!_aliases.TryGetValue(fullName, out alias))
            {
                alias = "_T" + _aliasIndex++;

                _aliases[fullName] = alias;
            }

            return alias;
        }

        /// <summary>
        /// Appends as clause.
        /// </summary>
        public StringBuilder AppendAsClause(StringBuilder builder, IFromClause clause)
        {
            Debug.Assert(builder != null);
            Debug.Assert(clause != null);

            var queryable = ExpressionWalker.GetCacheQueryable(clause);
            var tableName = ExpressionWalker.GetTableNameWithSchema(queryable);

            builder.AppendFormat("{0} as {1}", tableName, GetTableAlias(tableName));

            return builder;
        }
    }
}
