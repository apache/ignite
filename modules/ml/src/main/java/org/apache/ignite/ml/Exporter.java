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

package org.apache.ignite.ml;

/**
 * Exporter for models, D - model representation object, P - path to resources.
 */
public interface Exporter<D, P> {
    /**
     * Save model by path p
     *
     * @param d Model representation object.
     * @param p Path to resource.
     */
    public void save(D d, P p);

    /**
     * Load model representation object from p.
     *
     * @param p Path to resource.
     */
    public D load(P p);
}
