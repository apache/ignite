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

package org.apache.ignite.ioc;

import org.apache.ignite.IgniteCheckedException;

/**
 * Facade to other dependency injection mechanisms and bean registries.
 *
 * Purpose of this type is detachment of actual lookup from ignite specific SPI.
 * Both operations can be called by Ignite itself to initialize fields annotated with
 * {@link org.apache.ignite.resources.InjectResource}.
 *
 * @author ldywicki
 */
public interface Registry {

  <T> T lookup(Class<T> type);
  Object lookup(String name);
  Object unwrapTarget(Object target) throws IgniteCheckedException;

}
