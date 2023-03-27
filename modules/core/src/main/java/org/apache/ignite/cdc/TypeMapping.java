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

package org.apache.ignite.cdc;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.platform.PlatformType;

/**
 * Ignite maps type name to some id.
 * Mapping data required to perform deserialization of {@link BinaryObject}.
 * Note, {@link #typeName()} can represent not only Java class names.
 * For other supported platforms take a look at {@link PlatformType} values.
 *
 * @see BinaryObject
 * @see IgniteBinary#typeId(String)
 * @see BinaryIdMapper
 * @see CdcConsumer#onMappings(Iterator) 
 * @see MarshallerContext#registerClassName(byte, int, String, boolean) 
 */
@IgniteExperimental
public interface TypeMapping extends Serializable {
    /** @return Type id. */
    public int typeId();

    /** @return Type name. */
    public String typeName();

    /** @return Platform type. */
    public PlatformType platformType();
}
