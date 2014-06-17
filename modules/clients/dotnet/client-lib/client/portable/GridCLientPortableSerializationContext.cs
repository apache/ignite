/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    
    /**
     * <summary>Serialization context of current connection.</summary> 
     */ 
    class GridClientPortableSerializationContext
    {
        /** Cached metadatas. */
        private readonly ConcurrentDictionary<Type, Metadata> metas = new ConcurrentDictionary<Type,Metadata>();

        /** Pending metadata updates. */
        private readonly ConcurrentQueue<GridClientPortableClassMetadata> pendingMetas = new ConcurrentQueue<GridClientPortableClassMetadata>();

        /**
         * <summary></summary>
         * <param name="type">Type.</param>
         * <param name="fields">Fields.</param>
         */ 
        public GridClientPortableClassMetadata metadata(Type type, ICollection<string> fields)
        {
            Metadata meta = metas[type];

            GridClientPortableClassMetadata res;
            bool enqueue;

            if (meta == null) 
            {
                res = new GridClientPortableClassMetadata(type, fields);

                enqueue = true;
            }
            else 
                res = meta.Delta(fields, out enqueue);
                
            if (enqueue)
                pendingMetas.Enqueue(res);

            return res;
        }

        /**
         * <summary>Submit pending metadata to cached collection so that subsequent requests will not send it.</summary>
         */
        public void submitPendingMetadata()
        {
            GridClientPortableClassMetadata pendingMeta;

            while(pendingMetas.TryDequeue(out pendingMeta)) 
            {
                metas.AddOrUpdate(pendingMeta.Type, (type) => { return new Metadata(pendingMeta.Fields); }, (type, oldMeta) => { return oldMeta.Merge(pendingMeta.Fields); });
            }
        }

        /**
         * <summary>Metadata which is known to be sent to the client.</summary>
         */ 
        private class Metadata
        {
            /**
             * <summary>Constructor.</summary>
             * <param name="fields">Fields.</param>
             */ 
            public Metadata(ICollection<string> fields)
            {
                Fields = new HashSet<string>(fields);
            }

            /**
             * <summary>Fields.</summary>
             */ 
            public HashSet<string> Fields
            {
                get;
                private set;
            }

            /**
             * <summary>Merge new fields with metadata.</summary>
             * <param name="fields">Fields.</param>
             * <returns>Update metadata.</returns>
             */ 
            public Metadata Merge(ICollection<string> fields)
            {
                Fields.UnionWith(fields);

                return this;
            }

            /**
             * <summary>Create metadata for marshaller.</summary>
             * <param name="ctx">Context.</param>
             * <param name="fields">Participating fields.</param>
             * <returns>Metadata for mershaller.</returns>
             */
            public GridClientPortableClassMetadata Delta(ICollection<string> fields, out bool enqueue)
            {
                if (Fields.IsSupersetOf(fields))
                {
                    enqueue = false;

                    return GridClientPortableClassMetadata.EMPTY_META;
                }
                else
                {
                    enqueue = true;

                    HashSet<string> newFields = new HashSet<string>(fields);

                    newFields.ExceptWith(Fields);

                    return new GridClientPortableClassMetadata(null, newFields);
                }
            } 
        }
    }
}
