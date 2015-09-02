/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Resource
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Resource type descriptor.
    /// </summary>
    internal class ResourceTypeDescriptor
    {
        /** Attribute type: InstanceResourceAttribute. */
        private static readonly Type TYP_ATTR_GRID = typeof(InstanceResourceAttribute);

        /** Attribute type: StoreSessionResourceAttribute. */
        private static readonly Type TYP_ATTR_STORE_SES = typeof(StoreSessionResourceAttribute);

        /** Type: IGrid. */
        private static readonly Type TYP_GRID = typeof(IIgnite);

        /** Type: ICacheStoreSession. */
        private static readonly Type TYP_STORE_SES = typeof (ICacheStoreSession);

        /** Type: ComputeTaskNoResultCacheAttribute. */
        private static readonly Type TYP_COMPUTE_TASK_NO_RES_CACHE = typeof(ComputeTaskNoResultCacheAttribute);

        /** Cached binding flags. */
        private static readonly BindingFlags FLAGS = BindingFlags.Instance | BindingFlags.Public |
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Grid instance gridInjectors. */
        private readonly IList<IResourceInjector> gridInjectors;

        /** Session gridInjectors. */
        private readonly IList<IResourceInjector> storeSesInjectors;
        
        /** Task "no result cache" flag. */
        private readonly bool taskNoResCache;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param>
        internal ResourceTypeDescriptor(Type type)
        {
            Collector gridCollector = new Collector(TYP_ATTR_GRID, TYP_GRID);
            Collector storeSesCollector = new Collector(TYP_ATTR_STORE_SES, TYP_STORE_SES);

            Type curType = type;

            while (curType != null)
            {
                CreateInjectors(curType, gridCollector, storeSesCollector);

                curType = curType.BaseType;
            }

            gridInjectors = gridCollector.Injectors;
            storeSesInjectors = storeSesCollector.Injectors;

            taskNoResCache = ContainsAttribute(type, TYP_COMPUTE_TASK_NO_RES_CACHE, true);
        }

        /// <summary>
        /// Inject resources to the given object.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="grid">Grid.</param>
        public void InjectGrid(object target, GridImpl grid)
        {
            InjectGrid(target, grid.Proxy);
        }

        /// <summary>
        /// Inject resources to the given object.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="grid">Grid proxy.</param>
        public void InjectGrid(object target, GridProxy grid)
        {
            Inject0(target, grid, gridInjectors);
        }

        /// <summary>
        /// Inject store session.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="ses">Store session.</param>
        public void InjectStoreSession(object target, ICacheStoreSession ses)
        {
            Inject0(target, ses, storeSesInjectors);
        }

        /// <summary>
        /// Perform injection.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="injectee">Injectee.</param>
        /// <param name="injectors">Injectors.</param>
        private static void Inject0(object target, object injectee, ICollection<IResourceInjector> injectors)
        {
            if (injectors != null)
            {
                foreach (IResourceInjector injector in injectors)
                    injector.Inject(target, injectee);    
            }
        }

        /// <summary>
        /// Task "no result cache" flag.
        /// </summary>
        public bool TaskNoResultCache
        {
            get
            {
                return taskNoResCache;
            }
        }
        
        /// <summary>
        /// Create gridInjectors for the given type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="collectors">Collectors.</param>
        private static void CreateInjectors(Type type, params Collector[] collectors)
        {
            FieldInfo[] fields = type.GetFields(FLAGS);

            foreach (FieldInfo field in fields)
            {
                foreach (var collector in collectors)
                {
                    if (!ContainsAttribute(field, collector.AttributeType, false))
                        continue;

                    if (!field.FieldType.IsAssignableFrom(collector.ResourceType))
                        throw new IgniteException("Invalid field type for resource attribute [" + 
                            "type=" + type.Name +
                            ", field=" + field.Name + 
                            ", fieldType=" + field.FieldType.Name + 
                            ", resourceType=" + collector.ResourceType.Name + ']');

                    collector.Add(new ResourceFieldInjector(field));
                }
            }

            PropertyInfo[] props = type.GetProperties(FLAGS);

            foreach (var prop in props)
            {
                foreach (var collector in collectors)
                {
                    if (!ContainsAttribute(prop, collector.AttributeType, false))
                        continue;

                    if (!prop.CanWrite)
                        throw new IgniteException("Property with resource attribute is not writable [" +
                            "type=" + type.Name + 
                            ", property=" + prop.Name +
                            ", resourceType=" + collector.ResourceType.Name + ']');

                    if (!prop.PropertyType.IsAssignableFrom(collector.ResourceType))
                        throw new IgniteException("Invalid property type for resource attribute [" + 
                            "type=" + type.Name +
                            ", property=" + prop.Name + 
                            ", propertyType=" + prop.PropertyType.Name + 
                            ", resourceType=" + collector.ResourceType.Name + ']');

                    collector.Add(new ResourcePropertyInjector(prop));
                }
            }

            MethodInfo[] mthds = type.GetMethods(FLAGS);

            foreach (MethodInfo mthd in mthds)
            {
                foreach (var collector in collectors)
                {
                    if (!ContainsAttribute(mthd, collector.AttributeType, false)) 
                        continue;

                    ParameterInfo[] parameters = mthd.GetParameters();

                    if (parameters.Length != 1)
                        throw new IgniteException("Method with resource attribute must have only one parameter [" + 
                            "type=" + type.Name + 
                            ", method=" + mthd.Name +
                            ", resourceType=" + collector.ResourceType.Name + ']');

                    if (!parameters[0].ParameterType.IsAssignableFrom(collector.ResourceType))
                        throw new IgniteException("Invalid method parameter type for resource attribute [" +
                            "type=" + type.Name + 
                            ", method=" + mthd.Name + 
                            ", methodParameterType=" + parameters[0].ParameterType.Name + 
                            ", resourceType=" + collector.ResourceType.Name + ']');

                    collector.Add(new ResourceMethodInjector(mthd));
                }
            }
        }
        
        /// <summary>
        /// Check whether the given member contains the given attribute.
        /// </summary>
        /// <param name="member">Mmeber.</param>
        /// <param name="attrType">Attribute type.</param>
        /// <param name="inherit">Inherit flag.</param>
        /// <returns>True if contains</returns>
        private static bool ContainsAttribute(MemberInfo member, Type attrType, bool inherit)
        {
            return member.GetCustomAttributes(attrType, inherit).Length > 0;
        }

        /// <summary>
        /// Collector.
        /// </summary>
        private class Collector
        {
            /** Attribute type. */
            private readonly Type attrType;

            /** Resource type. */
            private readonly Type resType;
            
            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="attrType">Atrribute type.</param>
            /// <param name="resType">Resource type.</param>
            public Collector(Type attrType, Type resType)
            {
                this.attrType = attrType;
                this.resType = resType;
            }

            /// <summary>
            /// Attribute type.
            /// </summary>
            public Type AttributeType
            {
                get { return attrType; }
            }

            /// <summary>
            /// Resource type.
            /// </summary>
            public Type ResourceType
            {
                get { return resType; }
            }

            /// <summary>
            /// Add injector.
            /// </summary>
            /// <param name="injector">Injector.</param>
            public void Add(IResourceInjector injector)
            {
                if (Injectors == null)
                    Injectors = new List<IResourceInjector> { injector };
                else
                    Injectors.Add(injector);
            }

            /// <summary>
            /// Injectors.
            /// </summary>
            public List<IResourceInjector> Injectors { get; private set; }
        }
    }
}
