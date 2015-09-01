/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
{
    /// <summary>
    /// Object handle. Wraps a single value.
    /// </summary>
    internal class PortableObjectHandle
    {
        /** Value. */
        private readonly object val;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableObjectHandle"/> class.
        /// </summary>
        /// <param name="val">The value.</param>
        public PortableObjectHandle(object val)
        {
            this.val = val;
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public object Value
        {
            get { return val; }
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            var that = obj as PortableObjectHandle;

            return that != null && val == that.val;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return val != null ? val.GetHashCode() : 0;
        }
    }
}
