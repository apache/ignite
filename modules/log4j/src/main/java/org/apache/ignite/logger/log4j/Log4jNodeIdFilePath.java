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

package org.apache.ignite.logger.log4j;

import java.io.File;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Closure that generates file path adding node id to filename as a suffix.
 */
class Log4jNodeIdFilePath implements IgniteClosure<String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node id. */
    private final String postfix;

    /**
     * Creates new instance.
     *
     * @param postfix Postfix.
     */
    Log4jNodeIdFilePath(String postfix) {
        this.postfix = postfix;
    }

    /** {@inheritDoc} */
    @Override public String apply(String oldPath) {
        if (!F.isEmpty(U.IGNITE_LOG_DIR))
            return U.logFileName(postfix, new File(U.IGNITE_LOG_DIR, "ignite.log").getAbsolutePath());

        if (oldPath != null) // fileName could be null if IGNITE_HOME is not defined.
            return U.logFileName(postfix, oldPath);

        String tmpDir = IgniteSystemProperties.getString("java.io.tmpdir");

        if (tmpDir != null)
            return U.logFileName(postfix, new File(tmpDir, "ignite.log").getAbsolutePath());

        System.err.println("Failed to get tmp directory for log file.");

        return null;
    }
}
