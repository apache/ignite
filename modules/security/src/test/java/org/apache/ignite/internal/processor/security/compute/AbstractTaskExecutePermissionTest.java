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

package org.apache.ignite.internal.processor.security.compute;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.processor.security.AbstractPermissionTest;
import org.apache.ignite.lang.IgniteCallable;

/**
 *
 */
public abstract class AbstractTaskExecutePermissionTest extends AbstractPermissionTest {
    /** Jingle bell. */
    protected static final AtomicBoolean JINGLE_BELL = new AtomicBoolean(false);

    /** Allowed callable. */
    protected static final IgniteCallable<Object> ALLOWED_CALLABLE = () -> {
        JINGLE_BELL.set(true);

        return null;
    };

    /** Forbidden callable. */
    protected static final IgniteCallable<Object> FORBIDDEN_CALLABLE = () -> {
        fail("Should not be invoked.");

        return null;
    };

    /**
     * @param r TestRunnable.
     */
    protected void allowRun(TestRunnable r) {
        JINGLE_BELL.set(false);

        try {
            r.run();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue(JINGLE_BELL.get());
    }
}
