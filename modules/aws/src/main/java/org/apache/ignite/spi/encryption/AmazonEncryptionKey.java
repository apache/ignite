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

package org.apache.ignite.spi.encryption;

import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.MasterKey;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Amazon KMS encryption key implementation.
 *
 * @see AmazonEncryptionSpi
 */
public class AmazonEncryptionKey<M extends MasterKey<M>> extends DataKey<M> implements Serializable {
    /** Serializable version Id. */
    private static final long serialVersionUID = 0L;

    /**
     * @param key Encryption data key.
     */
    public AmazonEncryptionKey(DataKey<M> key) {
        super(key.getKey(), key.getEncryptedDataKey(), key.getProviderInformation(), key.getMasterKey());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        AmazonEncryptionKey<M> key = (AmazonEncryptionKey<M>)o;

        return Objects.equals(getKey(), key.getKey());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(getKey().getEncoded());
    }
}
