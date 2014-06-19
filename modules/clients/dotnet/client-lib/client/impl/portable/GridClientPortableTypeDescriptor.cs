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
         * <param name="typeId">Type ID.</param>
         * <param name="userType">User type flag.</param>
         * <param name="mapper">Mapper.</param>
         * <param name="serializer">Serializer</param>
         */
        public GridClientPortableTypeDescriptor(int typeId, bool userType, GridClientPortableIdMapper mapper, IGridClientPortableSerializer serializer)
        {
            TypeId = typeId;
            UserType = userType;
            Mapper = mapper;
            Serializer = serializer;
        }

        /**
         * <summary>Type ID.</summary>
         */
        readonly public int TypeId
        {
            get;
            private set;
        }

        /**
         * <summary>User type flag.</summary>
         */
        readonly public bool UserType
        {
            get;
            private set;
        }

        /**
         * <summary>Mapper.</summary>
         */
        readonly public GridClientPortableIdMapper Mapper
        {
            get;
            private set;
        }

        /**
         * <summary>Serializer.</summary>
         */
        readonly public IGridClientPortableSerializer Serializer
        {
            get;
            private set;
        }
    }
}
