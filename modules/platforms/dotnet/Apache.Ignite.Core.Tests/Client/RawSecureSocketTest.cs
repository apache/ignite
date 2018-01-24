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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Collections;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests the thin client mode with a raw secure socket.
    /// </summary>
    public class RawSecureSocketTest
    {
        // TODO: See queries_ssl_test.cpp, queries-ssl.xml
        
        
        [Test]
        public void TestSslOnServer()
        {
            // S:\W\incubator-ignite\modules\platforms\cpp\odbc-test\config\ssl\server.jks
            // IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH

            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH", 
                @"S:\W\incubator-ignite\modules\platforms\cpp\odbc-test\config");

            using (var ignite = Ignition.Start(
                @"S:\W\incubator-ignite\modules\platforms\cpp\odbc-test\config\queries-ssl.xml"))
            {
                var cfg = new IgniteClientConfiguration
                {
                    Host = "127.0.0.1",
                    Port = 11110
                };
                //using (var client = Ignition.StartClient(cfg))
                //{
                //    client.GetCacheNames();
                //}

                RunClient(cfg.Host, cfg.Host, cfg.Port);
            }
        }


        // The following method is invoked by the RemoteCertificateValidationDelegate.
        public static bool ValidateServerCertificate(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }

        public static void RunClient(string machineName, string serverName, int port)
        {
            // Create a TCP/IP client socket.
            // machineName is the host running the server application.
            var client = new TcpClient(machineName, port);
            Console.WriteLine("Client connected.");
            
            // Create an SSL stream that will close the client's stream.
            var sslStream = new SslStream(client.GetStream(), false, ValidateServerCertificate, null);

            // The server name must match the name on the server certificate.
            var certificate = new X509Certificate2(@"d:\mySrvKeystore.p12", "123456");
            var certsCollection = new X509CertificateCollection(new X509Certificate[] { certificate });

            try
            {
                sslStream.AuthenticateAsClient(serverName, certsCollection, SslProtocols.Default, false);
            }
            catch (AuthenticationException e)
            {
                Console.WriteLine("Exception: {0}", e.Message);
                if (e.InnerException != null)
                {
                    Console.WriteLine("Inner exception: {0}", e.InnerException.Message);
                }
                Console.WriteLine("Authentication failed - closing the connection.");
                client.Close();
                return;
            }

            // TODO: Handhsake.
            sslStream.WriteByte(1);

            client.Close();
            Console.WriteLine("Client closed.");
        }
    }
}
