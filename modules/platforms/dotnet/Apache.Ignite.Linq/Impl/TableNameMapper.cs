using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Ignite.Linq.Impl
{
    using Apache.Ignite.Core.Cache;

    internal static class TableNameMapper
    {
        public static string GetTableName<TKey, TValue>(ICache<TKey, TValue> cache)
        {
            return typeof (TValue).Name;
        }
    }
}
