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

package org.apache.ignite.console.web.security;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.crypto.keygen.BytesKeyGenerator;
import org.springframework.security.crypto.keygen.KeyGenerators;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.springframework.security.crypto.util.EncodingUtils.concatenate;
import static org.springframework.security.crypto.util.EncodingUtils.subArray;

/**
 * A {@code PasswordEncoder} implementation that uses PBKDF2.
 *
 * The algorithm is invoked on the concatenated bytes of the salt and password.
 */
public class PassportLocalPasswordEncoder implements PasswordEncoder {
    /** Key length, taken from passport-local-mongo */
    private static final int DEFAULT_HASH_WIDTH = 512 * 8;
    
    /** Number of hash iterations, taken from passport-local-mongo */
    private static final int DEFAULT_ITERATIONS = 25000;

    /** Salt generator. */
    private final BytesKeyGenerator saltGenerator = KeyGenerators.secureRandom(32);

    /** Hash width. */
    private final int hashWidth;

    /** Messages accessor. */
    private final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** Iterations. */
    private final int iterations;

    /**
     * Constructs a standard password encoder with a secret value which is also included in the password hash.
     */
    public PassportLocalPasswordEncoder() {
        this(DEFAULT_ITERATIONS, DEFAULT_HASH_WIDTH);
    }

    /**
     * Constructs a standard password encoder with a secret value as well as iterations and hash.
     *
     * @param iterations Number of iterations.
     * @param hashWidth Size of the hash.
     */
    public PassportLocalPasswordEncoder(int iterations, int hashWidth) {
        this.iterations = iterations;
        this.hashWidth = hashWidth;
    }

    /** {@inheritDoc} */
    @Override public String encode(CharSequence rawPwd) {
        byte[] salt = saltGenerator.generateKey();
        byte[] encoded = encode(rawPwd, salt);
        return encode(encoded);
    }

    /**
     * @param bytes Bytes.
     */
    private String encode(byte[] bytes) {
        return String.valueOf(Hex.encode(bytes));
    }

    /** {@inheritDoc} */
    @Override public boolean matches(CharSequence rawPwd, String encodedPwd) {
        byte[] digested = decode(encodedPwd);
        byte[] salt = subArray(digested, 0, saltGenerator.getKeyLength());
        
        return matches(digested, encode(rawPwd, salt));
    }

    /**
     * Constant time comparison to prevent against timing attacks.
     */
    private static boolean matches(byte[] exp, byte[] actual) {
        if (exp.length != actual.length)
            return false;

        int res = 0;

        for (int i = 0; i < exp.length; i++)
            res |= exp[i] ^ actual[i];

        return res == 0;
    }

    /**
     * @param encodedBytes Encoded bytes.
     */
    private byte[] decode(String encodedBytes) {
        return Hex.decode(encodedBytes);
    }

    /**
     * @param rawPwd Raw password.
     * @param salt Salt.
     */
    private byte[] encode(CharSequence rawPwd, byte[] salt) {
        try {
            PBEKeySpec spec = new PBEKeySpec(
                rawPwd.toString().toCharArray(),
                encode(salt).getBytes(StandardCharsets.UTF_8),
                iterations,
                hashWidth
            );

            SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            
            return concatenate(salt, skf.generateSecret(spec).getEncoded());
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException(messages.getMessage("err.could-not-create-hash"), e);
        }
    }
}
