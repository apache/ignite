using Apache.Ignite.Core.Cache.Query;

namespace dotnet_helloworld
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core;
    using GridGain.Core;
    using GridGain.Core.Security;

    public static class SqlJoinOrder
    {
        public static void EnforceJoinOrder()
        {
            // tag::sqlJoinOrder[]
            var query = new SqlFieldsQuery("SELECT * FROM TABLE_A, TABLE_B USE INDEX(HASH_JOIN_IDX) WHERE TABLE_A.column1 = TABLE_B.column2")
            {
                EnforceJoinOrder = true
            };
            // end::sqlJoinOrder[]
        }
    }

}