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

package org.apache.ignite.internal.management.encryption;

import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;

/** */
public class EncryptionChangeCacheKeyCommand implements ComputeCommand<EncryptionCacheGroupArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Change the encryption key of the cache group";
    }

    /** {@inheritDoc} */
    @Override public Class<EncryptionCacheGroupArg> argClass() {
        return EncryptionCacheGroupArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<ChangeCacheGroupKeyTask> taskClass() {
        return ChangeCacheGroupKeyTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(EncryptionCacheGroupArg arg, Void res, Consumer<String> printer) {
        printer.accept("The encryption key has been changed for the cache group \"" + arg.cacheGroupName() + "\".");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(EncryptionCacheGroupArg arg) {
        return "Warning: the command will change the encryption key of the cache group. Joining a node during " +
            "the key change process is prohibited and will be rejected.";
    }
}
