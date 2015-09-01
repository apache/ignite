﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Portable
{
    using System.Collections.Generic;
    
    /// <summary>
    /// Portable type metadata.
    /// </summary>
    public interface IPortableMetadata
    {
        /// <summary>
        /// Gets type name.
        /// </summary>
        /// <returns>Type name.</returns>
        string TypeName
        {
            get;
        }

        /// <summary>
        /// Gets field names for that type.
        /// </summary>
        /// <returns>Field names.</returns>
        ICollection<string> Fields
        {
            get;
        }

        /// <summary>
        /// Gets field type for the given field name.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field type.</returns>
        string FieldTypeName(string fieldName);

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        /// <returns>Affinity key field name or null in case it is not provided.</returns>
        string AffinityKeyFieldName
        {
            get;
        }
    }
}
