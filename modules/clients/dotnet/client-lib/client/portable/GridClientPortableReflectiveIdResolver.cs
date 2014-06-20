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
    using System.Collections.Generic;
    using System.Reflection;
    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;

    /**
     * <summary>ID mapper which uses reflection to calculate type/field IDs and delegates to hash code otherwise.</summary>
     */ 
    class GridClientPortableReflectiveIdResolver : GridClientPortableIdResolver 
    {
        /** Cached attribute type. */
        private static readonly Type ATTR;

        /** Cached binding flags. */
        private static readonly BindingFlags FLAGS = BindingFlags.Instance | BindingFlags.Public | 
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Type IDs. */
        private readonly IDictionary<string, int> typeIds = new Dictionary<string, int>();

        /** Field IDs. */
        private readonly IDictionary<KeyValuePair<int, string>, int> fieldIds = 
            new Dictionary<KeyValuePair<int, string>, int>();

        /**
         * <summary>Static initializer.</summary>
         */ 
        static GridClientPortableReflectiveIdResolver()
        {
            ATTR = typeof(GridClientPortableId);
        } 

        /** <inheritdoc /> */
        override public int? TypeId(string typeName)
        {
            int typeId;

            return typeIds.TryGetValue(typeName, out typeId) ? typeId : (int?)null;
        }

        /** <inheritdoc /> */
        override public int? FieldId(int typeId, string fieldName)
        {
            int fieldId;

            return fieldIds.TryGetValue(new KeyValuePair<int, string>(typeId, fieldName), out fieldId) ? 
                fieldId : (int?)null;
        }
        
        /**
         * <summary>Register particular type.</summary>
         * <param name="typeName">Type name.</param>
         */ 
        public void Register(string typeName)
        {
            if (typeIds.ContainsKey(typeName))
                throw new GridClientPortableException("Type already registered: " + typeName);
            else
            {
                try
                {
                    // 1. Get type ID.
                    Type type = Type.GetType(typeName);

                    object[] attrs = type.GetCustomAttributes(ATTR, false);

                    int typeId = attrs.Length > 0 ? ((GridClientPortableId)attrs[0]).Id :
                        GridClientPortableUilts.StringHashCode(type.Name.ToLower());

                    // 2. Detect collisions on type ID.
                    foreach (KeyValuePair<string, int> pair in typeIds)
                    {
                        if (typeId == pair.Value)
                            throw new GridClientPortableException("Conflicting type IDs [type1=" + pair.Key +
                                ", type2=" + type.Name + ", typeId=" + typeId + ']');
                    }

                    typeIds[typeName] = typeId;

                    // 3. Get field IDs.
                    Type curType = type;

                    while (curType != null)
                    {
                        foreach (FieldInfo fieldInfo in curType.GetFields(FLAGS))
                        {
                            if (!fieldInfo.IsNotSerialized)
                            {
                                object[] fieldAttrs = fieldInfo.GetCustomAttributes(ATTR, false);

                                int fieldId = fieldAttrs.Length > 0 ? ((GridClientPortableId)fieldAttrs[0]).Id :
                                    GridClientPortableUilts.StringHashCode(fieldInfo.Name.ToLower());

                                // 4. Detect collisions.
                                foreach (KeyValuePair<KeyValuePair<int, string>, int> pair in fieldIds)
                                {
                                    if (typeId == pair.Key.Key && fieldId == pair.Value)
                                        throw new GridClientPortableException("Conflicting field IDs [type=" +
                                                type.Name + ", field1=" + pair.Key.Value + ", field2=" + 
                                                fieldInfo.Name + ", fieldId=" + fieldId + ']');
                                }

                                fieldIds[new KeyValuePair<int, string>(typeId, fieldInfo.Name)] = fieldId;
                            }
                        }

                        curType = curType.BaseType;
                    }
                }
                catch (Exception e)
                {
                    throw new GridClientPortableException("Cannot instantiate type: " + typeName, e);
                }
            }
        }        
    }
}
