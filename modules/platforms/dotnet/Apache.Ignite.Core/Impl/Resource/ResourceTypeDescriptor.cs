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
        private static readonly Type TypAttrIgnite = typeof(InstanceResourceAttribute);

        /** Attribute type: StoreSessionResourceAttribute. */
        private static readonly Type TypAttrStoreSes = typeof(StoreSessionResourceAttribute);

        /** Attribute type: TaskSessionResourceAttribute. */
        private static readonly Type TypAttrTaskSes = typeof(TaskSessionResourceAttribute);
        
        /** Attribute type: TaskContinuousMapperResourceAttribute. */
        private static readonly Type TypTaskContinuousMapperResource = typeof(TaskContinuousMapperResourceAttribute);

        /** Type: IGrid. */
        private static readonly Type TypIgnite = typeof(IIgnite);

        /** Type: ICacheStoreSession. */
        private static readonly Type TypStoreSes = typeof (ICacheStoreSession);

        /** Type: IComputeTaskSession. */
        private static readonly Type TypTaskSes = typeof (IComputeTaskSession);

        /** Type: IComputeTaskContinuousMapper. */
        private static readonly Type TypComputeTaskContinuousMapper = typeof (IComputeTaskContinuousMapper);

        /** Type: ComputeTaskNoResultCacheAttribute. */
        private static readonly Type TypComputeTaskNoResCache = typeof(ComputeTaskNoResultCacheAttribute);

        /** Type: ComputeTaskSessionFullSupportAttribute. */
        private static readonly Type TypComputeTaskSessionFullSupport = typeof(ComputeTaskSessionFullSupportAttribute);

        /** Cached binding flags. */
        private const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public |
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Ignite injectors. */
        private readonly IList<IResourceInjector> _igniteInjectors;

        /** Session injectors. */
        private readonly IList<IResourceInjector> _storeSesInjectors;

        /** Compute task session injectors. */
        private readonly IList<IResourceInjector> _taskSesInjectors;

        /** Compute task continuous mapper injectors. */
        private readonly List<IResourceInjector> _taskMapperInjectors;

        /** Task "no result cache" flag. */
        private readonly bool _taskNoResCache;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param>
        internal ResourceTypeDescriptor(Type type)
        {
            Collector gridCollector = new Collector(TypAttrIgnite, TypIgnite);
            Collector storeSesCollector = new Collector(TypAttrStoreSes, TypStoreSes);
            var taskSesCollector = new Collector(TypAttrTaskSes, TypTaskSes);
            var taskMapperCollector = new Collector(TypTaskContinuousMapperResource, TypComputeTaskContinuousMapper);

            Type curType = type;

            while (curType != null)
            {
                CreateInjectors(curType, gridCollector, storeSesCollector, taskSesCollector, taskMapperCollector);

                curType = curType.BaseType;
            }

            _igniteInjectors = gridCollector.Injectors;
            _storeSesInjectors = storeSesCollector.Injectors;
            _taskSesInjectors = taskSesCollector.Injectors;
            _taskMapperInjectors = taskMapperCollector.Injectors;

            _taskNoResCache = ContainsAttribute(type, TypComputeTaskNoResCache, true);
            TaskSessionFullSupport = ContainsAttribute(type, TypComputeTaskSessionFullSupport, true);
        }

        /// <summary>
        /// Inject resources to the given object.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="ignite">Grid.</param>
        public void InjectIgnite(object target, IIgniteInternal ignite)
        {
            Inject0(target, ignite, _igniteInjectors);
        }

        /// <summary>
        /// Inject store session.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="ses">Store session.</param>
        public void InjectStoreSession(object target, ICacheStoreSession ses)
        {
            Inject0(target, ses, _storeSesInjectors);
        }

        /// <summary>
        /// Inject compute task session.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="ses">Compute task session.</param>
        public void InjectTaskSession(object target, IComputeTaskSession ses)
        {
            if (ses != null)
            {
                Inject0(target, ses, _taskSesInjectors);
            }
        }

        /// <summary>
        /// Inject compute task mapper.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="mapper">Compute task mapper.</param>
        public void InjectTaskMapper(object target, IComputeTaskContinuousMapper mapper)
        {
            if (mapper != null)
            {
                Inject0(target, mapper, _taskMapperInjectors);
            }
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
                return _taskNoResCache;
            }
        }

        /// <inheritdoc cref="ComputeTaskSessionFullSupportAttribute"/>
        public bool TaskSessionFullSupport { get; }

        /// <summary>
        /// Create gridInjectors for the given type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="collectors">Collectors.</param>
        private static void CreateInjectors(Type type, params Collector[] collectors)
        {
            FieldInfo[] fields = type.GetFields(Flags);

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

            PropertyInfo[] props = type.GetProperties(Flags);

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

            MethodInfo[] mthds = type.GetMethods(Flags);

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
            private readonly Type _attrType;

            /** Resource type. */
            private readonly Type _resType;
            
            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="attrType">Atrribute type.</param>
            /// <param name="resType">Resource type.</param>
            public Collector(Type attrType, Type resType)
            {
                _attrType = attrType;
                _resType = resType;
            }

            /// <summary>
            /// Attribute type.
            /// </summary>
            public Type AttributeType
            {
                get { return _attrType; }
            }

            /// <summary>
            /// Resource type.
            /// </summary>
            public Type ResourceType
            {
                get { return _resType; }
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
