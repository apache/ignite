/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Portable
{
    /// <summary>
    /// Maps class name and class field names to integer identifiers.
    /// </summary>
    public interface IPortableIdMapper
    {
        /// <summary>
        /// Gets type ID for the given type.
        /// </summary>
        /// <param name="typeName">Full type name.</param>
        /// <returns>ID of the class or 0 in case hash code is to be used.</returns>
        int TypeId(string typeName);

        /// <summary>
        /// Gets field ID for the given field of the given class.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="fieldName">Field name.</param>
        /// <returns>ID of the field or null in case hash code is to be used.</returns>
        int FieldId(int typeId, string fieldName);
    }
}
