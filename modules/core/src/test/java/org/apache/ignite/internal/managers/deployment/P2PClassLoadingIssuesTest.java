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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests for {@link P2PClassLoadingIssues}.
 */
public class P2PClassLoadingIssuesTest {
    /***/
    @Test
    public void p2pNoClassDefFoundErrorIsDisarmedToIgniteException() {
        try {
            P2PClassLoadingIssues.rethrowDisarmedP2PClassLoadingFailure(p2pClassLoadingError());

            fail("An exception should be thrown");
        }
        catch (IgniteException e) {
            // expected
        }
    }

    /***/
    @NotNull
    private NoClassDefFoundError p2pClassLoadingError() {
        NoClassDefFoundError error = new NoClassDefFoundError();
        error.initCause(new P2PClassNotFoundException("Oops"));
        return error;
    }

    /***/
    @NotNull
    private NoClassDefFoundError nonP2PClassLoadingError() {
        NoClassDefFoundError error = new NoClassDefFoundError();
        error.initCause(new ClassNotFoundException("Oops"));
        return error;
    }

    /***/
    @Test
    public void nonP2PNoClassDefFoundErrorIsRethrownAsIsWhenDisarming() {
        try {
            P2PClassLoadingIssues.rethrowDisarmedP2PClassLoadingFailure(nonP2PClassLoadingError());

            fail("An exception should be thrown");
        }
        catch (NoClassDefFoundError e) {
            // expected
        }
    }
}
