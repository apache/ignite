// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Text;
    using System.Diagnostics;
    using System.Globalization;
    using System.Collections.Generic;
    using System.Security.Cryptography.X509Certificates;

    using GridGain.Client.Ssl;
    
    using X = System.Console;

    /** <summary>Shows client authentication feature.</summary>*/
    /**
     * <summary>
     * Starts up an empty node with cache configuration.
     * You can also start a stand-alone GridGain instance by passing the path
     * to configuration file to <c>'ggstart.{sh|bat}'</c> script, like so:
     * <c>ggstart.sh examples/config/example-authentication-passcode.xml</c> or
     * <c>ggstart.sh examples/config/example-cache-authentication-passcode.xml</c>
     * to work with client without SSL (UseSsl=false) or
     * <c>ggstart.sh examples/config/example-cache-ssl.xml</c>
     * to work with client with SSL (UseSsl=true).
     * <para/>
     * Note that different nodes cannot share the same port for rest services. If you want
     * to start more than one node on the same physical machine you must provide different
     * configurations for each node. Otherwise, this example would not work.
     * <para/>
     * After node has been started this example creates a client.</summary>
     */
    public class GridAuthenticationAndSecureSessionExample {
        /** <summary>Grid node address to connect to.</summary> */
        public static readonly String ServerAddress = "127.0.0.1";

        /** <summary>Change this property to start example in SSL mode.</summary> */
        public static readonly bool UseSsl = false;

        /**
         * <summary>
         * Creates a C# client and authenticates it on server with credentials supplied.
         * If authentication passes, secure session is created for further communication
         * between client and server.
         * </summary>
         *
         * <param name="args">Command line arguments, none required.
         * The first argument interpreted as client credentials passcode.</param>
         */
        [STAThread]
        static void Main(string[] args) {
            String passcode = args.Length > 0 ? args[0] : "s3cret";

            try {
                IGridClient client = CreateClient(passcode);

                X.WriteLine(">>> Client successfully authenticated.");

                // Client is authenticated. You can add you code here, we will just show grid topology.
                X.WriteLine(">>> Current grid topology: " + String.Join(",", client.Compute().RefreshTopology(true, true)));

                // Command succeeded, session between client and grid node has been established.
                if (UseSsl)
                    X.WriteLine(">>> Secure session between client and grid has been established.");
                else
                    X.WriteLine(">>> Session between client and grid has been established.");

                //...
                //...
                //...
            }
            catch (GridClientAuthenticationException e) {
                X.WriteLine(">>> Failed to create client (was the passcode correct?): " + e.Message);
            }
            catch (GridClientException e) {
                X.WriteLine(">>> Failed to create client: " + e);
            }
            finally {
                GridClientFactory.StopAll(true);
            }
        }

        /**
         * <summary>
         * This method will create a client with default configuration. Note that this method expects that
         * first node will bind rest binary protocol on default port. It also expects that partitioned cache is
         * configured in grid.</summary>
         *
         * <param name="passcode">Passcode.</param>
         * <returns>Client instance.</returns>
         * <exception cref="GridClientException">If client could not be created.</exception>
         */
        private static IGridClient CreateClient(String passcode) {
            var partitioned = new GridClientDataConfiguration();

            // Set remote cache name.
            partitioned.Name = "partitioned";

            // Set client partitioned affinity for this cache.
            partitioned.Affinity = new GridClientPartitionedAffinity();

            var cc = new GridClientConfiguration();

            cc.DataConfigurations.Add(partitioned);

            // Point client to a local node.
            cc.Servers.Add(ServerAddress + ":" + GridClientConfiguration.DefaultTcpPort);

            // Set passcode credentials.
            cc.Credentials = passcode;

            // If we use ssl, set appropriate key- and trust-store.
            if (UseSsl) {
                var sslCtx = new GridClientSslContext();

                sslCtx.ClientCertificates.Add(new X509Certificate2("cert\\client.pfx", "123456"));

                cc.SslContext = sslCtx;
            }

            return GridClientFactory.Start(cc);
        }
    }
}
