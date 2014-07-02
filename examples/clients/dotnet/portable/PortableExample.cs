/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Example.Portable
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using GridGain.Client.Portable;

    using X = System.Console;

    public class PortableExample
    {
        /** <summary>Grid node address to connect to.</summary> */
        private static readonly String ServerAddress = "127.0.0.1";

        [STAThread]
        public static void Main()
        {
            IGridClient client = CreateClient();

            try
            {
                // Get remote cache.
                IGridClientData rmtCache = client.Data();

                // Put data to cache from .Net client.
                Employee emp1 = new Employee(Guid.NewGuid(), "Bill Gates", 10000);
                Employee emp2 = new Employee(Guid.NewGuid(), "Paul Allen", 20000);
                Employee emp3 = new Employee(Guid.NewGuid(), "Anders Hejlsberg", 40000);

                X.WriteLine("Put employee: " + emp1);
                rmtCache.Put<int, Employee>(4, emp1);

                X.WriteLine("Put employee: " + emp2);
                rmtCache.Put<int, Employee>(5, emp2);

                X.WriteLine("Put employee: " + emp3);
                rmtCache.Put<int, Employee>(6, emp3);

                // Get data from cache.
                ICollection<int> keys = new List<int>();

                keys.Add(4);
                keys.Add(5);
                keys.Add(6);

                X.WriteLine("\nReading data from cache:");

                IDictionary<int, Employee> emps = rmtCache.GetAll<int, Employee>(keys);

                foreach (KeyValuePair<int, Employee> pair in emps)
                    X.WriteLine(pair.Value);

                // Try getting data created by Java client.
                keys.Clear();

                keys.Add(1);
                keys.Add(2);
                keys.Add(3);

                emps = rmtCache.GetAll<int, Employee>(keys);

                if (emps.Count > 0)
                {
                    X.WriteLine("\nReading data from cache which was put by Java client:");

                    foreach (KeyValuePair<int, Employee> pair in emps)
                        X.WriteLine(pair.Value);
                }

                // Try getting data created by C client.
                keys.Clear();

                keys.Add(7);
                keys.Add(8);
                keys.Add(9);

                emps = rmtCache.GetAll<int, Employee>(keys);

                if (emps.Count > 0)
                {
                    X.WriteLine("\nReading data from cache which was put by C client:");

                    foreach (KeyValuePair<int, Employee> pair in emps)
                        X.WriteLine(pair.Value);
                }

            }
            catch (GridClientException e)
            {
                Console.WriteLine("Unexpected grid client exception : {0}", e);
            }
            finally
            {
                GridClientFactory.StopAll();
            }
        }

        /**
         * <summary>
         * This method will create a client with default configuration. Note that this method expects that
         * first node will bind rest binary protocol on default port.</summary>
         *
         * <returns>Client instance.</returns>
         * <exception cref="GridClientException">If client could not be created.</exception>
         */
        private static IGridClient CreateClient()
        {
            var cfg = new GridClientConfiguration();

            // Point client to a local node. Note that this server is only used
            // for initial connection. After having established initial connection
            // client will make decisions which grid node to use based on collocation
            // with key affinity or load balancing.
            cfg.Servers.Add(ServerAddress + ':' + GridClientConfiguration.DefaultTcpPort);
            
            cfg.DataConfigurations.Add(new GridClientDataConfiguration());

            GridClientPortableConfiguration portableCfg = new GridClientPortableConfiguration();

            GridClientPortableTypeConfiguration portableTypeCfg = new GridClientPortableTypeConfiguration(typeof(Employee));

            ICollection<GridClientPortableTypeConfiguration> portableTypeCfgs = 
                new List<GridClientPortableTypeConfiguration>(); 

            portableTypeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(Employee)));

            portableCfg.TypeConfigurations = portableTypeCfgs;

            cfg.PortableConfiguration = portableCfg;

            return GridClientFactory.Start(cfg);
        }
    }
}
