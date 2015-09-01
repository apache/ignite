/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Portable
{
    /// <summary>
    /// Maps type and field names to different names.
    /// </summary>
    public interface IPortableNameMapper
    {
        /// <summary>
        /// Gets the type name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>Type name.</returns>
        string TypeName(string name);

        /// <summary>
        /// Gets the field name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>Field name.</returns>
        string FieldName(string name);
    }
}
