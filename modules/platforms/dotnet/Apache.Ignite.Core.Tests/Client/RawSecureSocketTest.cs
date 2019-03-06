/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.IO;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Tests the thin client mode with a raw secure socket stream.
    /// </summary>
    public class RawSecureSocketTest
    {
        /// <summary>
        /// Tests that we can do handshake over SSL without using Ignite.NET APIs.
        /// </summary>
        [Test]
        public void TestHandshake()
        {
            var igniteConfiguration = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "Client", "server-with-ssl.xml")
            };

            using (Ignition.Start(igniteConfiguration))
            {
                const string host = "127.0.0.1";
                const int port = 11110;

                using (var client = new TcpClient(host, port))
                using (var sslStream = new SslStream(client.GetStream(), false, ValidateServerCertificate, null))
                {
                    var certsCollection = new X509CertificateCollection(new X509Certificate[] {LoadCertificateFile()});

                    sslStream.AuthenticateAsClient(host, certsCollection, SslProtocols.Tls, false);

                    Assert.IsTrue(sslStream.IsAuthenticated);
                    Assert.IsTrue(sslStream.IsMutuallyAuthenticated);
                    Assert.IsTrue(sslStream.IsEncrypted);

                    DoHandshake(sslStream);
                }
            }
        }

        /// <summary>
        /// Validates the server certificate.
        /// </summary>
        private static bool ValidateServerCertificate(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
            Console.WriteLine("Validating certificate: " + certificate);
            Console.WriteLine("Certificate errors: " + sslPolicyErrors);

            return true;
        }

        /// <summary>
        /// Loads the certificate file.
        /// </summary>
        private static X509Certificate2 LoadCertificateFile()
        {
            // Converting from JKS to PFX:
            // keytool -importkeystore -srckeystore thekeystore.jks -srcstoretype JKS
            // -destkeystore thekeystore.pfx -deststoretype PKCS12
            return new X509Certificate2(Path.Combine("Config", "Client", "thin-client-cert.pfx"), "123456");
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
