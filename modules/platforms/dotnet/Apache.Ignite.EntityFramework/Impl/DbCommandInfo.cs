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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System.Collections.Generic;
    using System.Data.Entity.Core.Common.CommandTrees;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    /// <summary>
    /// Command info.
    /// </summary>
    internal class DbCommandInfo
    {
        /** */
        private readonly bool _isModification;

        /** */
        private readonly DbCache _cache;

        /** */
        private readonly EntitySetBase[] _affectedEntitySets;

        /** */
        private readonly IDbCachingPolicy _policy;

        /** */
        private readonly DbTransactionInterceptor _txHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbCommandInfo"/> class.
        /// </summary>
        public DbCommandInfo(DbCommandTree tree, DbCache cache, IDbCachingPolicy policy, DbTransactionInterceptor txHandler)
        {
            Debug.Assert(tree != null);
            Debug.Assert(cache != null);
            Debug.Assert(txHandler != null);

            var qryTree = tree as DbQueryCommandTree;

            if (qryTree != null)
            {
                _isModification = false;

                _affectedEntitySets = GetAffectedEntitySets(qryTree.Query);
            }
            else
            {
                _isModification = true;

                var modify = tree as DbModificationCommandTree;

                if (modify != null)
                    _affectedEntitySets = GetAffectedEntitySets(modify.Target.Expression);
                else
                    // Functions (stored procedures) are not supported.
                    Debug.Assert(tree is DbFunctionCommandTree);
            }

            _cache = cache;
            _policy = policy;
            _txHandler = txHandler;
        }

        /// <summary>
        /// Gets a value indicating whether this command is a query and does not modify data.
        /// </summary>
        public bool IsModification
        {
            get { return _isModification; }
        }

        /// <summary>
        /// Gets or sets the cache.
        /// </summary>
        public DbCache Cache
        {
            get { return _cache; }
        }

        /// <summary>
        /// Gets the affected entity sets.
        /// </summary>
        public ICollection<EntitySetBase> AffectedEntitySets
        {
            get { return _affectedEntitySets; }
        }

        /// <summary>
        /// Gets the policy.
        /// </summary>
        public IDbCachingPolicy Policy
        {
            get { return _policy; }
        }

        /// <summary>
        /// Gets the tx handler.
        /// </summary>
        public DbTransactionInterceptor TxHandler
        {
            get { return _txHandler; }
        }

        /// <summary>
        /// Gets the affected entity sets.
        /// </summary>
        private static EntitySetBase[] GetAffectedEntitySets(DbExpression expression)
        {
            var visitor = new ScanExpressionVisitor();

            expression.Accept(visitor);

            // Should be sorted and unique.
            return visitor.EntitySets.ToArray();
        }

        /// <summary>
        /// Visits Scan expressions and collects entity set names.
        /// </summary>
        private class ScanExpressionVisitor : BasicCommandTreeVisitor
        {
            /** Unique and sorted entity sets. */
            private readonly SortedSet<EntitySetBase> _entitySets = 
                new SortedSet<EntitySetBase>(EntitySetComparer.Instance);

            /// <summary>
            /// Gets the entity sets.
            /// </summary>
            public IEnumerable<EntitySetBase> EntitySets
            {
                get { return _entitySets; }
            }

            /** <inheritdoc /> */
            public override void Visit(DbScanExpression expression)
            {
                _entitySets.Add(expression.Target);

                base.Visit(expression);
            }
        }

        /// <summary>
        /// Compares entity sets by name.
        /// </summary>
        private class EntitySetComparer : IComparer<EntitySetBase>
        {
            /** Default instance. */
            public static readonly EntitySetComparer Instance = new EntitySetComparer();

            /** <inheritdoc /> */
            [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
            public int Compare(EntitySetBase x, EntitySetBase y)
            {
                if (x == null && y == null)
                    return 0;

                if (x == null)
                    return -1;

                if (y == null)
                    return 1;

                return string.CompareOrdinal(x.Name, y.Name);
            }
        }
    }
}
