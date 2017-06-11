namespace Apache.Ignite.Linq.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;

    public static class EnumerableHelper
    {
        /// <summary>
        /// Gets item type of enumerable
        /// </summary>
        public static Type GetIEnumerableItemType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return type.GetGenericArguments()[0];
            }

            var implementedIEnumerableType = type.GetInterfaces()
                .FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            if (implementedIEnumerableType != null)
            {
                return implementedIEnumerableType.GetGenericArguments()[0];
            }

            if (type == typeof(IEnumerable))
            {
                return typeof(object);
            }

            throw new NotSupportedException("Type is not IEnumerable: " + type.FullName);
        }
    }
}