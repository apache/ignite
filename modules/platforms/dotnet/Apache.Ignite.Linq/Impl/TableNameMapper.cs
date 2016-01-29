using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Ignite.Linq.Impl
{
    using System.Diagnostics;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq.Clauses.Expressions;

    internal static class TableNameMapper
    {
        public static string GetTableName<TKey, TValue>(ICache<TKey, TValue> cache)
        {
            return GetTableNameFromEntryValueType(typeof (TValue));
        }

        public static string GetTableNameFromEntryValueType(Type entryValueType)
        {
            Debug.Assert(entryValueType != null);

            return entryValueType.Name;
        }

        public static string GetTableNameFromEntryType(Type cacheEntryType)
        {
            Debug.Assert(cacheEntryType != null);
            Debug.Assert(cacheEntryType.IsGenericType);
            Debug.Assert(cacheEntryType.GetGenericTypeDefinition() == typeof(ICacheEntry<,>));

            return GetTableNameFromEntryValueType(cacheEntryType.GetGenericArguments()[1]);
        }

        public static string GetTableName(QuerySourceReferenceExpression expression)
        {
            Debug.Assert(expression != null);

            return GetTableNameFromEntryType(expression.ReferencedQuerySource.ItemType);
        }

        public static string GetTableName(MemberExpression expression)
        {
            // TODO
            return "TODO";
        }


    }
}
