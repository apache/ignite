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

package org.apache.ignite.configuration.internal.selector;

import org.apache.ignite.configuration.internal.Modifier;

/**
 * Interface for objects helping select configuration elements.
 *
 * @param <ROOT> Root configuration class.
 * @param <TARGET> Target configuration class.
 * @param <VIEW> View class of target.
 * @param <INIT> Init class of target.
 * @param <CHANGE> Change class of target.
 */
public interface Selector<ROOT, TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> {
    /**
     * Select configuration element.
     *
     * @param root Configuration root object.
     * @return Configuration element.
     */
    TARGET select(ROOT root);
}
