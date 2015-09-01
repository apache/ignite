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
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Metadata for particular type.
    /// </summary>
    internal class PortableMetadataHolder
    {
        /** Type ID. */
        private readonly int typeId;

        /** Type name. */
        private readonly string typeName;

        /** Affinity key field name. */
        private readonly string affKeyFieldName;

        /** Empty metadata when nothig is know about object fields yet. */
        private readonly IPortableMetadata emptyMeta;

        /** Collection of know field IDs. */
        private volatile ICollection<int> ids;

        /** Last known unmodifiable metadata which is given to the user. */
        private volatile PortableMetadataImpl meta;

        /** Saved flag (set if type metadata was saved at least once). */
        private volatile bool saved;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        public PortableMetadataHolder(int typeId, string typeName, string affKeyFieldName)
        {
            this.typeId = typeId;
            this.typeName = typeName;
            this.affKeyFieldName = affKeyFieldName;

            emptyMeta = new PortableMetadataImpl(typeId, typeName, null, affKeyFieldName);
        }

        /// <summary>
        /// Get saved flag.
        /// </summary>
        /// <returns>True if type metadata was saved at least once.</returns>
        public bool Saved()
        {
            return saved;
        }

        /// <summary>
        /// Get current type metadata.
        /// </summary>
        /// <returns>Type metadata.</returns>
        public IPortableMetadata Metadata()
        {
            PortableMetadataImpl meta0 = meta;

            return meta0 != null ? meta : emptyMeta;
        }

        /// <summary>
        /// Currently cached field IDs.
        /// </summary>
        /// <returns>Cached field IDs.</returns>
        public ICollection<int> FieldIds()
        {
            ICollection<int> ids0 = ids;

            if (ids == null)
            {
                lock (this)
                {
                    ids0 = ids;

                    if (ids0 == null)
                    {
                        ids0 = new HashSet<int>();

                        ids = ids0;
                    }
                }
            }

            return ids0;
        }

        /// <summary>
        /// Merge newly sent field metadatas into existing ones.
        /// </summary>
        /// <param name="newMap">New field metadatas map.</param>
        public void Merge(IDictionary<int, Tuple<string, int>> newMap)
        {
            saved = true;

            if (newMap == null || newMap.Count == 0)
                return;

            lock (this)
            {
                // 1. Create copies of the old meta.
                ICollection<int> ids0 = ids;
                PortableMetadataImpl meta0 = meta;

                ICollection<int> newIds = ids0 != null ? new HashSet<int>(ids0) : new HashSet<int>();

                IDictionary<string, int> newFields = meta0 != null ?
                    new Dictionary<string, int>(meta0.FieldsMap()) : new Dictionary<string, int>(newMap.Count);

                // 2. Add new fields.
                foreach (KeyValuePair<int, Tuple<string, int>> newEntry in newMap)
                {
                    if (!newIds.Contains(newEntry.Key))
                        newIds.Add(newEntry.Key);

                    if (!newFields.ContainsKey(newEntry.Value.Item1))
                        newFields[newEntry.Value.Item1] = newEntry.Value.Item2;
                }

                // 3. Assign new meta. Order is important here: meta must be assigned before field IDs.
                meta = new PortableMetadataImpl(typeId, typeName, newFields, affKeyFieldName);
                ids = newIds;
            }
        }
    }
}
