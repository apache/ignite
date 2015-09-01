/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Portable
{
    /// <summary>
    /// Object handle dictionary for PortableReader.
    /// </summary>
    internal class PortableReaderHandleDictionary : PortableHandleDictionary<int, object>
    {
        /// <summary>
        /// Constructor with initial key-value pair.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public PortableReaderHandleDictionary(int key, object val)
            : base(key, val)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override int EmptyKey
        {
            get { return -1; }
        }
    }
}