namespace Apache.Ignite.Core.Impl.Client.Services
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client.Services;

    /// <summary>
    /// Implementation of client service descriptor.
    /// </summary>
    public class ClientServiceDescriptor : IClientServiceDescriptor
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="reader">Reader</param>
        public ClientServiceDescriptor(IBinaryRawReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            Name = reader.ReadString();
            ServiceClass = reader.ReadString();
            TotalCount = reader.ReadInt();
            MaxPerNodeCount = reader.ReadInt();
            CacheName = reader.ReadString();
            OriginNodeId = reader.ReadGuid();
            PlatformId = reader.ReadByte();
        }

        /** <inheritdoc /> */
        public string Name { get; private set; }

        /** <inheritdoc /> */
        public string ServiceClass { get; private set; }

        /** <inheritdoc /> */
        public int TotalCount { get; private set; }

        /** <inheritdoc /> */
        public int MaxPerNodeCount { get; private set; }

        /** <inheritdoc /> */
        public string CacheName { get; private set; }

        /** <inheritdoc /> */
        public Guid? OriginNodeId { get; private set; }

        /** <inheritdoc /> */
        public byte PlatformId { get; private set; }
    }
}
