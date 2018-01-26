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
    using System.IO;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Tests the thin client mode with a raw secure socket.
    /// </summary>
    public class RawSecureSocketTest
    {
        // TODO: See queries_ssl_test.cpp, queries-ssl.xml
        // https://ggsystems.atlassian.net/wiki/spaces/GG/pages/4219735/Set+up+SSL+connection+between+nodes


        [Test]
        public void TestSslOnServer()
        {
            // S:\W\incubator-ignite\modules\platforms\cpp\odbc-test\config\ssl\server.jks
            // IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH

            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH", 
                @"c:\w\incubator-ignite\modules\platforms\cpp\odbc-test\config");

            var icfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"c:\w\incubator-ignite\modules\platforms\cpp\odbc-test\config\queries-ssl.xml"
            };

            using (var ignite = Ignition.Start(icfg))
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
        private static bool ValidateServerCertificate(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
            return true;
            /**
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            return false;*/
        }

        private static void RunClient(string machineName, string serverName, int port)
        {
            // Create a TCP/IP client socket.
            // machineName is the host running the server application.
            var client = new TcpClient(machineName, port);
            Console.WriteLine("Client connected.");
            
            // Create an SSL stream that will close the client's stream.
            var sslStream = new SslStream(client.GetStream(), false, ValidateServerCertificate, null);

            // The server name must match the name on the server certificate.
            var certificate = LoadCertificateFile();
            var certsCollection = new X509CertificateCollection(new X509Certificate[] { certificate });

            try
            {
                sslStream.AuthenticateAsClient(serverName, certsCollection, SslProtocols.Default, false);
            }
            catch (AuthenticationException)
            {
                Console.WriteLine("Authentication failed - closing the connection.");
                client.Close();
                throw;
            }

            Assert.IsTrue(sslStream.IsAuthenticated);
            Assert.IsTrue(sslStream.IsMutuallyAuthenticated);
            Assert.IsTrue(sslStream.IsEncrypted);

            DoHandshake(sslStream);

            client.Close();
            Console.WriteLine("Client closed.");
        }

        private static X509Certificate2 LoadCertificateFile()
        {
            // File has been created with the command:
            // openssl pkcs12 -export -out cert.pfx -in client_full.pem -certfile ca.pem

            // Instead we can convert from JKS directly with 
            // keytool -importkeystore -srckeystore thekeystore.jks -srcstoretype JKS -destkeystore thekeystore.pfx -deststoretype PKCS12

            // TODO: What is the use case? Ask Igor.
            // 1) User generates some certificates for server and client ?
            // 2) Certificates are applied in Java style on server, pfx on .NET client?
            // What do we add to IgniteClientConfiguration? 

            return new X509Certificate2(@"Config\thin-client-cert.pfx", "123456");
        }

        /// <summary>
        /// Does the handshake.
        /// </summary>
        /// <param name="sock">The sock.</param>
        private static void DoHandshake(Stream sock)
        {
            SendRequest(sock, stream =>
            {
                // Handshake.
                stream.WriteByte(1);

                // Protocol version.
                stream.WriteShort(1);
                stream.WriteShort(0);
                stream.WriteShort(0);

                // Client type: platform.
                stream.WriteByte(2);
            });

            // ACK.
            var ack = ReceiveMessage(sock);

            Assert.AreEqual(1, ack.Length);
            Assert.AreEqual(1, ack[0]);
        }


        /// <summary>
        /// Receives the message.
        /// </summary>
        private static byte[] ReceiveMessage(Stream sock)
        {
            var buf = new byte[4];
            sock.Read(buf, 0, 4);

            using (var stream = new BinaryHeapStream(buf))
            {
                var size = stream.ReadInt();
                buf = new byte[size];
                sock.Read(buf, 0, size);
                return buf;
            }
        }

        /// <summary>
        /// Sends the request.
        /// </summary>
        private static void SendRequest(Stream sock, Action<BinaryHeapStream> writeAction)
        {
            using (var stream = new BinaryHeapStream(128))
            {
                stream.WriteInt(0);  // Reserve message size.

                writeAction(stream);

                stream.WriteInt(0, stream.Position - 4);  // Write message size.

                sock.Write(stream.GetArray(), 0, stream.Position);
            }
        }

    }
}
