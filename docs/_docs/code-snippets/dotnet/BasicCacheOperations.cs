using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Compute;

namespace dotnet_helloworld
{
    public class BasicCacheOperations
    {
        public static void AtomicOperations()
        {
            // tag::atomicOperations1[]
            using (var ignite = Ignition.Start("examples/config/example-cache.xml"))
            {
                var cache = ignite.GetCache<int, string>("cache_name");

                for (var i = 0; i < 10; i++)
                {
                    cache.Put(i, i.ToString());
                }

                for (var i = 0; i < 10; i++)
                {
                    Console.Write("Got [key=" + i + ", val=" + cache.Get(i) + ']');
                }
            }
            // end::atomicOperations1[]

            // tag::atomicOperations2[]
            using (var ignite = Ignition.Start("examples/config/example-cache.xml"))
            {
                var cache = ignite.GetCache<string, int>("cache_name");

                // Put-if-absent which returns previous value.
                var oldVal = cache.GetAndPutIfAbsent("Hello", 11);

                // Put-if-absent which returns boolean success flag.
                var success = cache.PutIfAbsent("World", 22);

                // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
                oldVal = cache.GetAndReplace("Hello", 11);

                // Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
                success = cache.Replace("World", 22);

                // Replace-if-matches operation.
                success = cache.Replace("World", 2, 22);

                // Remove-if-matches operation.
                success = cache.Remove("Hello", 1);
            }
            // end::atomicOperations2[]
        }

        // tag::asyncExec[]
        class HelloworldFunc : IComputeFunc<string>
        {
            public string Invoke()
            {
                return "Hello World";
            }
        }
        
        public static void AsynchronousExecution()
        {
            var ignite = Ignition.Start();
            var compute = ignite.GetCompute();
            
            //Execute a closure asynchronously
            var fut = compute.CallAsync(new HelloworldFunc());
            
            // Listen for completion and print out the result
            fut.ContinueWith(Console.Write);
        }
        // end::asyncExec[]
    }
}