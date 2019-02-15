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

namespace Apache.Ignite.Core.Ssl
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// This SSL context factory that provides ssl context configuration with specified key and trust stores.
    /// </summary>
    [Serializable]
    public class SslContextFactory : ISslContextFactory
    {
        /// <summary> Default value for <see cref="KeyAlgorithm"/>. </summary>
        public const string DefaultKeyAlgorithm = "SunX509";

        /// <summary> Default value for <see cref="KeyStoreType"/> and <see cref="TrustStoreType"/>. </summary>
        public const string DefaultStoreType = "JKS";

        /// <summary> Default value for <see cref="Protocol"/>. </summary>
        public const string DefaultSslProtocol = "TLS";

        /// <summary>
        /// Key manager algorithm that will be used to create a key manager. Notice that in most cased default value 
        /// <see cref="DefaultKeyAlgorithm"/> suites well, however, on Android platform this value need to be set 
        /// to X509.
        /// </summary>
        public string KeyAlgorithm { get; set; }

        /// <summary>
        /// Key store type used for context creation. <see cref="DefaultStoreType"/> by default.
        /// </summary>
        public string KeyStoreType { get; set; }

        /// <summary>
        /// Key store file path.
        /// </summary>
        public string KeyStoreFilePath { get; set; }

        /// <summary>
        /// Key store file password.
        /// </summary>
        public string KeyStorePassword { get; set; }

        /// <summary>
        /// Protocol for secure transport. <see cref="DefaultSslProtocol"/> by default.
        /// </summary>
        public string Protocol { get; set; }

        /// <summary>
        /// Path to trust store file. Could be null if any SSL Certificate should be accepted/succeed.
        /// </summary>
        public string TrustStoreFilePath { get; set; }

        /// <summary>
        /// Trust store password.
        /// </summary>
        public string TrustStorePassword { get; set; }

        /// <summary>
        /// Trust store type used for context creation. <see cref="DefaultStoreType"/> by default.
        /// </summary>
        public string TrustStoreType { get; set; }

        /// <summary>
        /// Creates a new instance of the <see cref="SslContextFactory"/> class.
        /// </summary>
        /// <param name="keyStoreFilePath">Path to key store file.</param>
        /// <param name="keyStorePassword">Key store password.</param>
        /// <param name="trustStoreFilePath">Path to trust store file.</param>
        /// <param name="trustStorePassword">Trust store password.</param>
        public SslContextFactory(string keyStoreFilePath, string keyStorePassword,
                                 string trustStoreFilePath, string trustStorePassword) 
            : this(keyStoreFilePath, keyStorePassword)
        {
            TrustStoreFilePath = trustStoreFilePath;
            TrustStorePassword = trustStorePassword;
        }

        /// <summary>
        /// Creates a new instance of the <see cref="SslContextFactory"/> class.
        /// Trust store file is not set that results in accepting any SSL Certificate.
        /// </summary>
        /// <param name="keyStoreFilePath">Path to key store file.</param>
        /// <param name="keyStorePassword">Key store password.</param>
        public SslContextFactory(string keyStoreFilePath, string keyStorePassword) : this()
        {
            KeyStoreFilePath = keyStoreFilePath;
            KeyStorePassword = keyStorePassword;
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        public SslContextFactory()
        {
            KeyAlgorithm = DefaultKeyAlgorithm;
            TrustStoreType = DefaultStoreType;
            KeyStoreType = DefaultStoreType;
            Protocol = DefaultSslProtocol;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SslContextFactory"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal SslContextFactory(IBinaryRawReader reader)
        {
            Read(reader);
        }

        /// <summary>
        /// Reads data into this instance from the specified reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        private void Read(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            KeyAlgorithm = reader.ReadString();

            KeyStoreType = reader.ReadString();
            KeyStoreFilePath = reader.ReadString();
            KeyStorePassword = reader.ReadString();

            Protocol = reader.ReadString();

            TrustStoreType = reader.ReadString();
            TrustStoreFilePath = reader.ReadString();
            TrustStorePassword = reader.ReadString();
        }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            Debug.Assert(writer != null);

            writer.WriteString(KeyAlgorithm);

            writer.WriteString(KeyStoreType);
            writer.WriteString(KeyStoreFilePath);
            writer.WriteString(KeyStorePassword);

            writer.WriteString(Protocol);

            writer.WriteString(TrustStoreType);
            writer.WriteString(TrustStoreFilePath);
            writer.WriteString(TrustStorePassword);
        }
    }
}
