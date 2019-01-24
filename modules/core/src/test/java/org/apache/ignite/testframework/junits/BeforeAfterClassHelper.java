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

package org.apache.ignite.testframework.junits;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper for Junit 4 test classes that need to use instance-specific actions along with BeforeClass and AfterClass
 * annotations which require static methods. IMPL NOTE it relies on assumption that classes using this helper
 * will be executed by a test framework serially. Also note that we can't expect methods of this class to be
 * invoked from the same thread.
 */
public class BeforeAfterClassHelper {
    /** */
    private final AtomicReference<Callable<Void>> afterClsActRef = new AtomicReference<>(null);

    /** */
    private final AtomicBoolean isFirst = new AtomicBoolean();

    /**
     * Must be invoked from containing class in the method annotated with {@code BeforeClass}.
     */
    public void onBeforeClass() {
        isFirst.set(true);
    }

    /**
     * Must be invoked from containing class in the method annotated with {@code Before}, or, if class uses
     * {@code Rule}, in a place that guarantees it to be invoked prior to test routine evaluation.
     *
     * @param beforeClsAct Action to execute on first time before test cases or {@code null} to do nothing.
     * @param afterClsAct Action to execute on last time after test cases or {@code null} to do nothing.
     */
    public void onBefore(Callable<Void> beforeClsAct, Callable<Void> afterClsAct) throws Exception {
        if (!isFirst.getAndSet(false))
            return;

        afterClsActRef.set(afterClsAct);

        if (beforeClsAct != null)
            beforeClsAct.call();
    }

    /**
     * Must be invoked from containing class in the method annotated with {@code AfterClass}.
     */
    public void onAfterClass() throws Exception {
        try {
            Callable<Void> afterClsAct = afterClsActRef.get();

            if (afterClsAct != null)
                afterClsAct.call();
        }
        finally {
            afterClsActRef.set(null);
        }
    }
}
