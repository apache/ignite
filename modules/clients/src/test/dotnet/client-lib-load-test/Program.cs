// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain {
    using System;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Collections.Generic;
    using System.Security.Cryptography.X509Certificates;
    using GridGain.Client;
    using GridGain.Client.Ssl;

    using G = GridGain.Client.GridClientFactory;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Java client load tests.</summary> */
    class GridClientLoadTest {
        static void Main(string[] args) {
            Dbg.Listeners.Add(new System.Diagnostics.TextWriterTraceListener(System.Console.Out));
            Dbg.AutoFlush = true;

            // Bypass all certificates.
            System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, cert, chain, error) => true;

            var clientsCount = 20;
            var threadsCount = 20;

            var sslCtx = new GridClientSslContext(true);

            sslCtx.ClientCertificates.Add(new X509Certificate2("cert\\client.pfx", "123456"));

            var cfgs = new IGridClientConfiguration[] {
                CreateConfig(GridClientProtocol.Tcp, null, "127.0.0.1:10080"),
                CreateConfig(GridClientProtocol.Tcp, sslCtx, "127.0.0.1:10443"),
                CreateConfig(GridClientProtocol.Http, null, "127.0.0.1:11080"),
                CreateConfig(GridClientProtocol.Http, sslCtx, "127.0.0.1:11443"),
            };

            var clients = new List<IGridClient>();

            try {
                Dbg.WriteLine("Start clients");

                while (clients.Count < clientsCount)
                    foreach (var cfg in cfgs)
                        clients.Add(G.Start(cfg));

                Dbg.WriteLine("Started clients: {0}", clients.Count);

                var taskName = "org.gridgain.client.GridClientHttpTask";
                var taskArgs = "[\"asdf\"]";

                var threads = new List<Thread>();
                var closed = false;

                while (threads.Count < threadsCount) {
                    var t = new Thread(() => {
                        Random rnd = new Random();
                        IGridClient client;

                        while (!closed) {
                            lock (clients) {
                                client = clients[rnd.Next(clients.Count)];
                            }

                            try {
                                var data = client.Data();
                                var exec = client.Compute();

                                var futs = new List<IGridClientFuture>();

                                //Dbg.WriteLine(Thread.CurrentThread.Name);

                                futs.Add(data.PutAsync<String, String>("key", "value"));
                                futs.Add(data.GetAsync<String, String>("key"));
                                futs.Add(exec.ExecuteAsync<int>(taskName, taskArgs));
                                futs.Add(exec.ExecuteAsync<int>(taskName, taskArgs));

                                foreach (var fut in futs)
                                    fut.WaitDone();
                            }
                            catch (GridClientServerUnreachableException e) {
                                Dbg.WriteLine("Server unavailable: " + e.Message);

                                lock (clients) {
                                    clients.Remove(client);
                                }

                                G.Stop(client.Id);
                            }
                            catch (GridClientClosedException e) {
                                Dbg.WriteLine("Client closed [clientId={0}, e={1}]", client.Id, e);

                                lock (clients) {
                                    clients.Remove(client);
                                }

                                G.Stop(client.Id);
                            }
                            catch (GridClientException e) {
                                Dbg.WriteLine("Grid exception happens: " + e);
                            }
                            catch (Exception e) {
                                Dbg.WriteLine("Unexpected exception happens: " + e);

                                closed = true;

                                break;
                            }
                        }
                    });

                    t.Name = "test-thread-" + threads.Count;

                    threads.Add(t);
                }

                var stat = new Thread(() => {
                    while (true) {
                        Thread.Sleep(TimeSpan.FromSeconds(10));

                        IDictionary<ThreadState, int> map = new Dictionary<ThreadState, int>();

                        foreach (var state in (ThreadState[])Enum.GetValues(typeof(ThreadState)))
                            map[state] = 0;

                        foreach (var t in threads)
                            map[t.ThreadState]++;

                        StringBuilder b = new StringBuilder(DateTime.Now.ToString());

                        foreach (var pair in map)
                            b.Append("  ").Append(pair.Key).Append("=").Append(pair.Value);

                        Dbg.WriteLine(b.ToString());
                    }
                });

                stat.IsBackground = true;
                stat.Start();

                Dbg.WriteLine("Start threads: {0}", threads.Count);

                foreach (var t in threads)
                    t.Start();

                Dbg.WriteLine("Threads started: {0}", threads.Count);

                while (true) {
                    Console.Out.WriteLine("Press 'q' key to exit...");

                    if (Console.ReadKey(true).Key == ConsoleKey.Q) {
                        Console.Out.WriteLine("\nAre you sure (y/n)?");

                        if (Console.ReadKey(false).Key == ConsoleKey.Y)
                            break;
                    }
                }

                closed = true;

                Dbg.WriteLine("\nStop threads: {0}", threads.Count);

                for (int i = 0; i < threads.Count; i++) {
                    var t = threads[i];
                    if (t.IsAlive) {
                        Dbg.WriteLine("Join thread #{0}", i);

                        //t.Join();
                        if (!t.Join(TimeSpan.FromSeconds(5))) {
                            Dbg.WriteLine("Thread #{0} join timed out. Thread aborted.", i);
                            t.Abort();
                        }

                        Dbg.WriteLine("Thread #{0} stopped", i);
                    }
                    else
                        Dbg.WriteLine("Thread #{0} already stopped", i);
                }

                Dbg.WriteLine("\nAll threads stopped");
            }
            finally {
                Dbg.WriteLine("Stop clients: {0}", clients.Count);
                G.StopAll(true);
                Dbg.WriteLine("Stoped clients: {0}", clients.Count);
            }
        }

        private static IGridClientConfiguration CreateConfig(GridClientProtocol proto, IGridClientSslContext sslCtx, String srv) {
            var replicated = new GridClientDataConfiguration();
            replicated.Name = "replicated";

            var partitoned = new GridClientDataConfiguration();
            partitoned.Name = "partitioned";
            partitoned.Affinity = new GridClientPartitionAffinity();

            GridClientConfiguration cfg = new GridClientConfiguration();

            cfg.DataConfigurations.Add(replicated);
            cfg.DataConfigurations.Add(partitoned);

            cfg.Protocol = proto;
            cfg.SslContext = sslCtx;
            cfg.Servers.Add(srv);
            cfg.Credentials = "s3cret";

            return cfg;
        }
    }
}
