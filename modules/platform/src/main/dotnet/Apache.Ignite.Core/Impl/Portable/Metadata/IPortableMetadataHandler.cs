/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Portable.Metadata
{
    using System.Collections.Generic;

    /// <summary>
    /// Portable metadata handler.
    /// </summary>
    public interface IPortableMetadataHandler
    {
        /// <summary>
        /// Callback invoked when named field is written.
        /// </summary>
        /// <param name="fieldId">Field ID.</param>
        /// <param name="fieldName">Field name.</param>
        /// <param name="typeId">Field type ID.</param>
        void OnFieldWrite(int fieldId, string fieldName, int typeId);

        /// <summary>
        /// Callback invoked when object write is finished and it is time to collect missing metadata.
        /// </summary>
        /// <returns>Collected metadata.</returns>
        IDictionary<string, int> OnObjectWriteFinished();
    }
}
