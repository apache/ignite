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

namespace Apache.Ignite.Core.SwapSpace.File
{
    using System;
    using System.ComponentModel;

    /// <summary>
    /// File-based swap space SPI implementation which holds keys in memory and values on disk.
    /// It is intended for cases when value is bigger than 100 bytes, otherwise it will not 
    /// have any positive effect.
    /// </summary>
    public class FileSwapSpaceSpi : ISwapSpaceSpi
    {
        /// <summary>
        /// Default value for <see cref="MaximumSparsity"/> property.
        /// </summary>
        public const float DefaultMaximumSparsity = 0.5f;

        /// <summary>
        /// Default value for <see cref="WriteBufferSize"/> property.
        /// </summary>
        public const int DefaultWriteBufferSize = 64 * 1024;

        /// <summary>
        /// Default value for <see cref="MaximumWriteQueueSize"/> property.
        /// </summary>
        public const int DefaultMaximumWriteQueueSize = 1024 * 1024;

        /// <summary>
        /// Default value for <see cref="ReadStripesNumber"/> property.
        /// </summary>
        public static readonly int DefaultReadStripesNumber = Environment.ProcessorCount;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSwapSpaceSpi"/> class.
        /// </summary>
        public FileSwapSpaceSpi()
        {
            MaximumSparsity = DefaultMaximumSparsity;
            MaximumWriteQueueSize = DefaultMaximumWriteQueueSize;
            ReadStripesNumber = DefaultReadStripesNumber;
            WriteBufferSize = DefaultWriteBufferSize;
        }

        /// <summary>
        /// Gets or sets the base directory.
        /// </summary>
        public string BaseDirectory { get; set; }

        /// <summary>
        /// Gets or sets the maximum sparsity. This property defines maximum acceptable
        /// wasted file space to whole file size ratio.
        /// When this ratio becomes higher than specified number compacting thread starts working.
        /// </summary>
        /// <value>
        /// The maximum sparsity. Must be between 0 and 1.
        /// </value>
        [DefaultValue(DefaultMaximumSparsity)]
        public float MaximumSparsity { get; set; }

        /// <summary>
        /// Gets or sets the maximum size of the write queue in bytes. If there are more values are waiting
        /// to be written to disk then specified size, SPI will block on write operation.
        /// </summary>
        /// <value>
        /// The maximum size of the write queue, in bytes.
        /// </value>
        [DefaultValue(DefaultMaximumWriteQueueSize)]
        public int MaximumWriteQueueSize { get; set; }

        /// <summary>
        /// Gets or sets the read stripes number. Defines number of file channels to be used concurrently. 
        /// Default is <see cref="Environment.ProcessorCount"/>.
        /// </summary>
        /// <value>
        /// Number of read stripes.
        /// </value>
        public int ReadStripesNumber { get; set; }

        /// <summary>
        /// Gets or sets the size of the write buffer, in bytes. Write to disk occurs only when this buffer is full.
        /// </summary>
        /// <value>
        /// The size of the write buffer, in bytes.
        /// </value>
        [DefaultValue(DefaultWriteBufferSize)]
        public int WriteBufferSize { get; set; }
    }
}
