using System;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Expiry;
using Apache.Ignite.Core.Common;

namespace dotnet_helloworld
{
    class ExpiryPolicies
    {

        // tag::cfg[]
        class ExpiryPolicyFactoryImpl : IFactory<IExpiryPolicy>
        {
            public IExpiryPolicy CreateInstance()
            {
                return new ExpiryPolicy(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100),
                    TimeSpan.FromMilliseconds(100));
            }
        }

        public static void Example()
        {
            var cfg = new CacheConfiguration
            {
                Name = "cache_name",
                ExpiryPolicyFactory = new ExpiryPolicyFactoryImpl()
            };
            // end::cfg[]
        }

        public static void EagerTtl()
        {
            // tag::eagerTTL[]
            var cfg = new CacheConfiguration
            {
                Name = "cache_name",
                EagerTtl = true
            };
            // end::eagerTTL[]
        }

    }
}
