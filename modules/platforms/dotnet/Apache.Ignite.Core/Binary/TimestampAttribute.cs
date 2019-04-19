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

namespace Apache.Ignite.Core.Binary
{
    using System;

    /// <summary>
    /// Instructs the serializer to write DateTime fields and properties in Timestamp format,
    /// which is interoperable with other platforms and works in SQL,
    /// but does not allow non-UTC values.
    /// <para />
    /// When applied to a struct or a class, changes behavior for all fields and properties.
    /// <para />
    /// Normally serializer uses <see cref="IBinaryWriter.WriteObject{T}"/> for DateTime fields.
    /// This attribute changes the behavior to <see cref="IBinaryWriter.WriteTimestamp"/>.
    /// <para />
    /// See also <see cref="BinaryReflectiveSerializer.ForceTimestamp"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property | 
        AttributeTargets.Class | AttributeTargets.Struct)]
    public sealed class TimestampAttribute : Attribute
    {
        // No-op.
    }
}
