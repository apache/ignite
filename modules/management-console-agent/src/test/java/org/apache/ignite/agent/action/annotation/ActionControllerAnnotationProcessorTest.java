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

package org.apache.ignite.agent.action.annotation;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.agent.action.ActionMethod;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Action controller annotation processor.
 */
public class ActionControllerAnnotationProcessorTest {
    /**
     * Should find the tests action controllers.
     */
    @Test
    public void findActionMethods() {
        Map<String, ActionMethod> methods = ActionControllerAnnotationReader.findActionMethods("org.apache.ignite.agent.action");

        assertTrue(methods.containsKey("TestActionController.action"));

        assertTrue(methods.containsKey("TestAction.action"));
    }

    /**
     * Test action controller.
     */
    @ActionController
    private static class TestActionController {
        /**
         * @param flag Flag.
         */
        public static CompletableFuture<Boolean> action(boolean flag) {
            return CompletableFuture.completedFuture(flag);
        }
    }

    /**
     * Test action controller with name.
     */
    @ActionController("TestAction")
    private static class TestActionControllerWithName {
        /**
         * @param flag Flag.
         */
        public static CompletableFuture<Boolean> action(boolean flag) {
            return CompletableFuture.completedFuture(flag);
        }
    }
}
