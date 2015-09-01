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
    using System.Collections.Generic;

    /// <summary>
    /// Metadata handler which uses hash set to determine whether field was already written or not.
    /// </summary>
    internal class PortableHashsetMetadataHandler : IPortableMetadataHandler
    {
        /** Empty fields collection. */
        private static readonly IDictionary<string, int> EMPTY_FIELDS = new Dictionary<string, int>();

        /** IDs known when serialization starts. */
        private readonly ICollection<int> ids;

        /** New fields. */
        private IDictionary<string, int> fieldMap;

        /** */
        private readonly bool newType;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ids">IDs.</param>
        /// <param name="newType">True is metadata for type is not saved.</param>
        public PortableHashsetMetadataHandler(ICollection<int> ids, bool newType)
        {
            this.ids = ids;
            this.newType = newType;
        }

        /** <inheritdoc /> */
        public void OnFieldWrite(int fieldId, string fieldName, int typeId)
        {
            if (!ids.Contains(fieldId))
            {
                if (fieldMap == null)
                    fieldMap = new Dictionary<string, int>();

                if (!fieldMap.ContainsKey(fieldName))
                    fieldMap[fieldName] = typeId;
            }
        }

        /** <inheritdoc /> */
        public IDictionary<string, int> OnObjectWriteFinished()
        {
            return fieldMap ?? (newType ? EMPTY_FIELDS : null);
        }
    }
}
