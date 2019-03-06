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

namespace Apache.Ignite.Core.Encryption.Keystore
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// IEncryptionSPI implementation base on JDK provided cipher algorithm implementations.
    /// </summary>
    public class KeystoreEncryptionSpi : IEncryptionSpi
    {
        /// <summary>
        /// Default master key name.
        /// </summary>
        public const string DefaultMasterKeyName = "ignite.master.key";

        /// <summary>
        /// Default encryption key size.
        /// </summary>
        public const int DefaultKeySize = 256;
        
        /// <summary>
        /// Name of master key in key store.
        /// </summary>
        [DefaultValue(DefaultMasterKeyName)]
        public string MasterKeyName { get; set; }
        
        /// <summary>
        /// Size of encryption key.
        /// </summary>
        [DefaultValue(DefaultKeySize)]
        public int KeySize { get; set; }
        
        /// <summary>
        /// Path to key store.
        /// </summary>
        public string KeyStorePath { get; set; }

        /// <summary>
        /// Key store password.
        /// </summary>
        public string KeyStorePassword { get; set; }

        /// <summary>
        /// Empty constructor.
        /// </summary>
        public KeystoreEncryptionSpi()
        {
            MasterKeyName = DefaultMasterKeyName;
            KeySize = DefaultKeySize;
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="KeystoreEncryptionSpi"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public KeystoreEncryptionSpi(IBinaryRawReader reader)
        {
            MasterKeyName = reader.ReadString();
            KeySize = reader.ReadInt();
            KeyStorePath = reader.ReadString();

            var keyStorePassword = reader.ReadCharArray();

            KeyStorePassword = keyStorePassword == null ? null : new string(keyStorePassword);
        }
    }
}
