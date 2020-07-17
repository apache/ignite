using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    class OnHeapCaching
    {
        public static void Run()
        {
            // tag::onheap[]
            var cfg = new CacheConfiguration
            {
                Name = "myCache",
                OnheapCacheEnabled = true
            };
            // end::onheap[]
        }

    }
}
