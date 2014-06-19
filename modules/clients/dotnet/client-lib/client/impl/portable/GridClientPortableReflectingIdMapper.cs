/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */


namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using GridGain.Client.Portable;

    /**
     * <summary>ID mapper which uses reflection to calculate type/field IDs and delegates to hash code otherwise.</summary>
     */ 
    class GridClientPortableReflectingIdMapper : GridClientPortableIdMapper 
    {
        /** Cached attribute type. */
        private static readonly Type ATTR;

        /** Cached binding flags. */
        private static readonly BindingFlags FLAGS = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Type. */
        private readonly Type type;

        /** Type ID. */
        private readonly int? typeId;

        /** Field type IDs. */
        private readonly IDictionary<string, int> fieldIds = new Dictionary<string, int>();

        /**
         * <summary>Static initializer.</summary>
         */ 
        static GridClientPortableReflectingIdMapper()
        {
            ATTR = typeof(GridClientPortableId);
        } 

        /** 
         * <summary>Constructor.</summary>
         * <param name="type">Type.</param>
         */ 
        public GridClientPortableReflectingIdMapper(Type type)
        {
            this.type = type;

            // Get type ID.
            object[] attrs = type.GetCustomAttributes(ATTR, false);

            if (attrs.Length > 0)
                typeId = ((GridClientPortableId)attrs[0]).Id;

            // Get field IDs.
            Type curType = type;

            while (curType != null)
            {
                foreach (FieldInfo fieldInfo in curType.GetFields(FLAGS))
                {
                    if (!fieldInfo.IsNotSerialized)
                    {
                        object[] fieldAttrs = fieldInfo.GetCustomAttributes(ATTR, false);

                        if (fieldAttrs.Length > 0)
                        {
                            int fieldId = ((GridClientPortableId)fieldAttrs[0]).Id;

                            // Early collision detection.
                            foreach (KeyValuePair<string, int> pair in fieldIds)
                            {
                                if (fieldId == pair.Value)
                                    throw new GridClientPortableException("Conflicting field IDs [type=" + 
                                        type.Name + ", field1=" + pair.Key + ", field2=" + fieldInfo.Name + 
                                        ", fieldId=" + fieldId + ']');
                            }

                            fieldIds[fieldInfo.Name] = ((GridClientPortableId)fieldAttrs[0]).Id;
                        }
                    }
                }
                
                curType = curType.BaseType;
            }

        }

        /** <inheritdoc /> */
        override public int? TypeId(string className)
        {
            return typeId;
        }

        /** <inheritdoc /> */
        override public int? FieldId(int typeId, string fieldName)
        {
            return fieldIds[fieldName];
        }
    }
}
