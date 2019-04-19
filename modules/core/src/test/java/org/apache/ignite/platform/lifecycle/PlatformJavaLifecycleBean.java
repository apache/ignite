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

package org.apache.ignite.platform.lifecycle;

import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;

/**
 * Java lifecycle bean.
 */
public class PlatformJavaLifecycleBean implements LifecycleBean {
    /** Count of "beforeStart" invocations. */
    public static volatile int beforeStartCnt;

    /** Count of "afterStart" invocations. */
    public static volatile int afterStartCnt;

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        switch (evt) {
            case BEFORE_NODE_START:
                beforeStartCnt++;

                break;

            case AFTER_NODE_START:
                afterStartCnt++;

                break;
        }
    }
}