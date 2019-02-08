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

package org.apache.ignite.failure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.GridAbstractTest;

import static org.apache.ignite.testframework.junits.GridAbstractTest.testIsRunning;

/**
 * Stops node and fails test.
 */
public class TestFailingFailureHandler extends StopNodeFailureHandler {
    /** Expected throwables. */
    private static final List<T2<Class<? extends Throwable>, String>> expectedErrs = new ArrayList<>();

    /** Test. */
    @GridToStringExclude
    protected final GridAbstractTest test;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log; //TODO remove before PA

    /**
     * @param test Test.
     */
    public TestFailingFailureHandler(GridAbstractTest test, IgniteLogger log) {
        this.test = test;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
        log.info("asd123 handle error="+failureCtx.error().getClass()+", cause="+failureCtx.error().getCause().getClass().getSimpleName());

        if (isFailureExpected(failureCtx))
            return false;

        if (!testIsRunning.getAndSet(false)) {
            ignite.log().info("Critical issue detected after test finished. Test failure handler ignore it.");

            return true;
        }

        boolean nodeStopped = super.handle(ignite, failureCtx);

        test.handleFailure(failureCtx.error());

        return nodeStopped;
    }

    /**
     * @param failureCtx Failure context.
     * @return True - if critical failure is expected.
     */
    private boolean isFailureExpected(FailureContext failureCtx) {
        for (T2<Class<? extends Throwable>, String> err : expectedErrs) {
            if (!X.hasCause(failureCtx.error(), err.get2(), err.get1()))
                continue;

            if (log.isInfoEnabled())
                log.info("Caught expected exception: " + failureCtx.error());

            return true;
        }
        return false;
    }

    /**
     * @param cls Expected throwable.
     * @return {@code true} (as specified by {@link Collection#add})
     */
    public static boolean expect(Class<? extends Throwable> cls) {
        return expect(cls, null);
    }

    /**
     * @param cls Expected throwable.
     * @param msg Expected message.
     * @return {@code true} (as specified by {@link Collection#add})
     */
    public static boolean expect(Class<? extends Throwable> cls, String msg) {
        return expectedErrs.add(new T2<>(cls, msg));
    }

    /**
     * @param cls Expected throwable.
     * @return {@code true} (as specified by {@link Collection#remove})
     */
    public static boolean unExpect(Class<? extends Throwable> cls) {
        return unExpect(cls, "");
    }

    /**
     * @param cls Expected throwable.
     * @param msg Expected message.
     * @return {@code true} (as specified by {@link Collection#remove})
     */
    public static boolean unExpect(Class<? extends Throwable> cls, String msg) {
        for (T2<Class<? extends Throwable>, String> err : expectedErrs) {
            if (err.get1().equals(cls) && err.get2().equals(msg))
                return expectedErrs.remove(err);
        }

        return false;
    }

    /**
     * Expect no failures.
     */
    public static void clear() {
        expectedErrs.clear();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TestFailingFailureHandler.class, this);
    }
}
