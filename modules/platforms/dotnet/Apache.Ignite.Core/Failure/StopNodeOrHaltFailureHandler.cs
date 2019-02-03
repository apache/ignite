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

namespace Apache.Ignite.Core.Failure
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Handler will try to stop node if <see cref="TryStop"/> value is true.
    /// If node can't be stopped during provided <see cref="Timeout"/> or <see cref="TryStop"/> value is false
    /// then JVM process will be terminated forcibly.
    /// <para />
    /// </summary>
    public class StopNodeOrHaltFailureHandler : IFailureHandler
    {
        /// <summary>
        /// Try stop flag.
        /// Defaults false.
        /// </summary>
        [DefaultValue(false)]
        public bool TryStop { get; set; }
        
        
        /// <summary>
        /// Stop node timeout.
        /// Defaults to 0.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "0:0:0")]
        public TimeSpan Timeout { get; set; }

        /// <summary>
        /// Initialize a new instance of the <see cref="StopNodeOrHaltFailureHandler"/>
        /// </summary>
        public StopNodeOrHaltFailureHandler()
        {
            TryStop = false;
            Timeout = TimeSpan.Zero;
        }

        /// <summary>
        /// Reads instance.
        /// </summary>
        internal static StopNodeOrHaltFailureHandler Read(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            var tryStop = reader.ReadBoolean();
            var timeout = reader.ReadLong();

            return new StopNodeOrHaltFailureHandler
            {
                TryStop = tryStop,
                Timeout = timeout < 0 || timeout > TimeSpan.MaxValue.TotalMilliseconds
                          ? TimeSpan.Zero 
                          : TimeSpan.FromMilliseconds(timeout)
            };
        }

        /// <summary>
        /// Writes this instance.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteBoolean(TryStop);

            if (Timeout == TimeSpan.MaxValue)
            {
                writer.WriteLong(long.MaxValue);
            }
            else if (Timeout < TimeSpan.Zero)
            {
                writer.WriteLong(0);
            }
            else
            {
                writer.WriteLong((long)Timeout.TotalMilliseconds);
            }
        }
    }
}
