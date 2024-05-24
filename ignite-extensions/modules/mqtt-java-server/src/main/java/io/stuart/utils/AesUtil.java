/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.utils;

import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.digest.DigestAlgorithm;
import cn.hutool.crypto.digest.Digester;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;
import io.stuart.consts.CacheConst;

public class AesUtil {

    private static final AES aes;

    static {
        Digester md5 = new Digester(DigestAlgorithm.MD5);

        // md5 bytes
        byte[] raw = md5.digest(CacheConst.SYS_AES_KEY);
        // get key bytes
        byte[] key = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), raw).getEncoded();

        aes = SecureUtil.aes(key);
    }

    public static String encryptBase64(String src) {
        return aes.encryptBase64(src);
    }


}
