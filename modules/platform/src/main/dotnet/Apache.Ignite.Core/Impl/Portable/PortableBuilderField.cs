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
    using System;

    /// <summary>
    /// Portable builder field.
    /// </summary>
    internal class PortableBuilderField
    {
        /** Remove marker object. */
        public static readonly object RMV_MARKER_OBJ = new object();

        /** Remove marker. */
        public static readonly PortableBuilderField RMV_MARKER = 
            new PortableBuilderField(null, RMV_MARKER_OBJ);

        /** Type. */
        private readonly Type typ;

        /** Value. */
        private readonly object val;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="val">Value.</param>
        public PortableBuilderField(Type typ, object val)
        {
            this.typ = typ;
            this.val = val;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public Type Type
        {
            get
            {
                return typ;
            }
        }

        /// <summary>
        /// Value.
        /// </summary>
        public object Value
        {
            get
            {
                return val;
            }
        }
    }
}
