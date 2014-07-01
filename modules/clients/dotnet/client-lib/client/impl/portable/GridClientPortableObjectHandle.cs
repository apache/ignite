/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections.Generic;

    /**
     * <summary>Object handle. Wraps a single value.</summary>
     */ 
    internal struct GridClientPortableObjectHandle
    {
        /** Value. */
        private object val;

        /**
         * <summary>Constructor.</summary>
         * <param name="val">Value.</param>
         */ 
        public GridClientPortableObjectHandle(object val)
        {
            this.val = val;
        }

        /**
         * <summary>Value.</summary>
         */
        public object Value
        {
            get { return val; }
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (obj != null && obj is GridClientPortableObjectHandle)
            {
                GridClientPortableObjectHandle that = (GridClientPortableObjectHandle)obj;

                return Value == that.Value;
            }
            else
                return false;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return Value != null ? Value.GetHashCode() : 0;
        }
    }
}
