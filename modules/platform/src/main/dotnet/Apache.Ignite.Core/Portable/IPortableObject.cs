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
    /// Wrapper for serialized portable objects.
    /// </summary>
    public interface IPortableObject
    {
        /// <summary>Gets portable object type ID.</summary>
        /// <returns>Type ID.</returns>
        int TypeId();

        /// <summary>
        /// Gets object metadata.
        /// </summary>
        /// <returns>Metadata.</returns>
        IPortableMetadata Metadata();

        /// <summary>Gets field value.</summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field value.</returns>
        F Field<F>(string fieldName);

        /// <summary>Gets fully deserialized instance of portable object.</summary>
        /// <returns>Fully deserialized instance of portable object.</returns>
        T Deserialize<T>();
    }
}
