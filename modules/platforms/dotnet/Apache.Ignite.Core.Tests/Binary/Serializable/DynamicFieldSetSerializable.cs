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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Tests.Client.Cache;

    /// <summary>
    /// Serializable class with dynamic field set: some fields are serialized based on a condition.
    /// </summary>
    public class DynamicFieldSetSerializable : ISerializable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EmptyObject"/> class.
        /// </summary>
        public DynamicFieldSetSerializable()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EmptyObject"/> class.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        // ReSharper disable once UnusedParameter.Local
        public DynamicFieldSetSerializable(SerializationInfo info, StreamingContext context)
        {
            WriteFoo = info.GetBoolean("WriteFoo");
            if (WriteFoo)
            {
                Foo = info.GetInt32("Foo");
            }
            
            WriteBar = info.GetBoolean("WriteBar");
            if (WriteBar)
            {
                Bar = info.GetString("Bar");
            }
        }
        
        /** <inheritdoc /> */
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("WriteFoo", WriteFoo);
            info.AddValue("WriteBar", WriteBar);

            if (WriteFoo)
            {
                info.AddValue("Foo", Foo);
            }
            
            if (WriteBar)
            {
                info.AddValue("Bar", Bar);
            }
        }

        public int Foo { get; set; }
        
        public string Bar { get; set; }
        
        public bool WriteFoo { get; set; }
        
        public bool WriteBar { get; set; }
    }
}