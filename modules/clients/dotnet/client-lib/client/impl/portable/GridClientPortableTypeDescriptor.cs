using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GridGain.Client.Impl.Portable
{
    using GridGain.Client.Portable;

    /**
     * <summary>Type descriptor.</summary>
     */ 
    class GridClientPortableTypeDescriptor
    {
        /**
         * <param name="type">Type.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="userType">User type flag.</param>
         * <param name="mapper">Mapper.</param>
         * <param name="serializer">Serializer</param>
         */
        public GridClientPortableTypeDescriptor(Type type, int typeId, bool userType, 
            GridClientPortableIdResolver mapper, IGridClientPortableSerializer serializer)
        {
            Type = type;
            TypeId = typeId;
            UserType = userType;
            Mapper = mapper;
            Serializer = serializer;
        }

        /**
        * <summary>Type.</summary>
        */
        public Type Type
        {
            get;
            private set;
        }

        /**
         * <summary>Type ID.</summary>
         */
        public int TypeId
        {
            get;
            private set;
        }

        /**
         * <summary>User type flag.</summary>
         */
        public bool UserType
        {
            get;
            private set;
        }

        /**
         * <summary>Mapper.</summary>
         */
        public GridClientPortableIdResolver Mapper
        {
            get;
            private set;
        }

        /**
         * <summary>Serializer.</summary>
         */
        public IGridClientPortableSerializer Serializer
        {
            get;
            private set;
        }
    }
}
