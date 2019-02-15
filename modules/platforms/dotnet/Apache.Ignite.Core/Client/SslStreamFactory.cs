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

namespace Apache.Ignite.Core.Client
{
    using System.ComponentModel;
    using System.IO;
    using System.Net.Security;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Predefined SSL stream factory, loads certificate from specified file.
    /// </summary>
    public class SslStreamFactory : ISslStreamFactory
    {
        /// <summary>
        /// Default SSL protocols.
        /// </summary>
        public const SslProtocols DefaultSslProtocols = SslProtocols.Tls;

        /// <summary>
        /// Initializes a new instance of the <see cref="SslStreamFactory"/> class.
        /// </summary>
        public SslStreamFactory()
        {
            SslProtocols = DefaultSslProtocols;
        }

        /** <inehritdoc /> */
        public SslStream Create(Stream stream, string targetHost)
        {
            IgniteArgumentCheck.NotNull(stream, "stream");

            var sslStream = new SslStream(stream, false, ValidateServerCertificate, null);

            var cert = new X509Certificate2(CertificatePath, CertificatePassword);
            var certs = new X509CertificateCollection(new X509Certificate[] { cert });

            sslStream.AuthenticateAsClient(targetHost, certs, SslProtocols, CheckCertificateRevocation);

            return sslStream;
        }

        /// <summary>
        /// Validates the server certificate.
        /// </summary>
        private bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, 
            SslPolicyErrors sslPolicyErrors)
        {
            if (SkipServerCertificateValidation)
            {
                return true;
            }

            if (sslPolicyErrors == SslPolicyErrors.None)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Gets or sets the certificate file (pfx) path.
        /// <para />
        /// Java certificates can be converted with <c>keytool</c>:
        /// <c>keytool -importkeystore -srckeystore thekeystore.jks -srcstoretype JKS
        /// -destkeystore thekeystore.pfx -deststoretype PKCS12</c>
        /// </summary>
        public string CertificatePath { get; set; }

        /// <summary>
        /// Gets or sets the certificate file password.
        /// </summary>
        public string CertificatePassword { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to ignore invalid remote (server) certificates.
        /// This may be useful for testing with self-signed certificates.
        /// </summary>
        public bool SkipServerCertificateValidation { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the certificate revocation list is checked during authentication.
        /// </summary>
        public bool CheckCertificateRevocation { get; set; }

        /// <summary>
        /// Gets or sets the SSL protocols.
        /// </summary>
        [DefaultValue(DefaultSslProtocols)]
        public SslProtocols SslProtocols { get; set; }
    }
}
