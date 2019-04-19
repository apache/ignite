/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
