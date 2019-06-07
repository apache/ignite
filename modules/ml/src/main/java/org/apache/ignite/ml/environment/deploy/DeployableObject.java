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

package org.apache.ignite.ml.environment.deploy;

import java.util.List;

/**
 * Represents an final objects from Ignite ML library like models or preprocessors having
 * dependencies that can be custom objects from client side.
 *
 * NOTE: this interface should be inherited only by Ignite ML classes.
 */
public interface DeployableObject {
    /**
     * Returns dependencies of this object that can be object with class defined by client side and unknown for server.
     */
    public List<Object> getDependencies();
}
