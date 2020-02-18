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

#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0
namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System;
    using System.Diagnostics;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// AppDomain accessor.
    /// <para />
    /// Logic is borrowed from internal .NET Framework classes.
    /// See System.Deployment.Application.NativeMethods.
    /// </summary>
    internal static class AppDomains
    {
        // ReSharper disable InconsistentNaming
        // ReSharper disable UnusedMember.Global
        /** */
        private static Guid CLSID_CLRMetaHost = new Guid(
            0x9280188d, 0xe8e, 0x4867, 0xb3, 0xc, 0x7f, 0xa8, 0x38, 0x84, 0xe8, 0xde);

        /** */
        private static Guid IID_CLRMetaHost = new Guid(
            0xD332DB9E, 0xB9B3, 0x4125, 0x82, 0x07, 0xA1, 0x48, 0x84, 0xF5, 0x32, 0x16);

        /** */
        private static Guid IID_CLRRuntimeInfo = new Guid(
            0xBD39D1D2, 0xBA2F, 0x486a, 0x89, 0xB0, 0xB4, 0xB0, 0xCB, 0x46, 0x68, 0x91);

        /** */
        private static Guid CLSID_CorRuntimeHost = new Guid(
            0xcb2f6723, 0xab3a, 0x11d2, 0x9c, 0x40, 0x00, 0xc0, 0x4f, 0xa3, 0x0a, 0x3e);

        /** */
        private static Guid IID_CorRuntimeHost = new Guid(
            0xcb2f6722, 0xab3a, 0x11d2, 0x9c, 0x40, 0x00, 0xc0, 0x4f, 0xa3, 0x0a, 0x3e);

        /// <summary>
        /// Gets the default application domain.
        /// </summary>
        public static _AppDomain GetDefaultAppDomain()
        {
            if (Os.IsMono)
            {
                var prop = typeof(AppDomain).GetProperty(
                    "DefaultDomain", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
                
                Debug.Assert(prop != null);
                
                return (_AppDomain) prop.GetValue(null, null);
            }

            object objHost;
            int hr = NativeMethods.CLRCreateInstance(ref CLSID_CLRMetaHost, ref IID_CLRMetaHost, out objHost);

            if (hr < 0)
            {
                throw new IgniteException("Failed to get default AppDomain. Cannot create meta host: " + hr);
            }

            var host = (ICLRMetaHost) objHost;
            var vers = Environment.Version;
            var versString = string.Format("v{0}.{1}.{2}", vers.Major, vers.Minor, vers.Build);
            var runtime = (ICLRRuntimeInfo) host.GetRuntime(versString, ref IID_CLRRuntimeInfo);

            bool started;
            uint flags;
            runtime.IsStarted(out started, out flags);
            Debug.Assert(started);

            var rtHost = (ICorRuntimeHost) runtime.GetInterface(ref CLSID_CorRuntimeHost, ref IID_CorRuntimeHost);

            _AppDomain domain;
            rtHost.GetDefaultDomain(out domain);

            return domain;
        }

        [ComImport, Guid("D332DB9E-B9B3-4125-8207-A14884F53216"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        private interface ICLRMetaHost
        {
            [return: MarshalAs(UnmanagedType.Interface)]
            object GetRuntime(string version, ref Guid iid);
        }

        [ComImport, Guid("BD39D1D2-BA2F-486a-89B0-B4B0CB466891"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        private interface ICLRRuntimeInfo
        {
            void GetVersionString(char[] buffer, int bufferLength);
            void GetRuntimeDirectory(char[] buffer, int bufferLength);
            bool IsLoaded(IntPtr hProcess);
            void LoadErrorString(uint id, char[] buffer, int bufferLength, int lcid);
            void LoadLibrary(string path, out IntPtr hMdodule);
            void GetProcAddress(string name, out IntPtr addr);
            [return: MarshalAs(UnmanagedType.Interface)]
            object GetInterface(ref Guid clsid, ref Guid iid);
            bool IsLoadable();
            void SetDefaultStartupFlags(uint flags, string configFile);
            void GetDefaultStartupFlags(out uint flags, char[] configFile, int configFileLength);
            void BindAsLegacyV2Runtime();
            void IsStarted(out bool started, out uint flags);
        }

        [ComImport, Guid("CB2F6722-AB3A-11d2-9C40-00C04FA30A3E"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        private interface ICorRuntimeHost
        {
            void CreateLogicalThreadState();
            void DeleteLogicalThreadState();
            void SwitchinLogicalThreadState(IntPtr cookie);
            void SwitchoutLogicalThreadState(out IntPtr cookie);
            void LocksHeldByLogicalThread(out int count);
            void MapFile(IntPtr hFile, out IntPtr address);
            void GetConfiguration(out IntPtr config);
            void Start();
            void Stop();
            void CreateDomain(string name, object identity, out _AppDomain domain);
            void GetDefaultDomain(out _AppDomain domain);
        }

        private static class NativeMethods
        {
            [DllImport("mscoree.dll")]
            public static extern int CLRCreateInstance(ref Guid clsid, ref Guid iid,
                [MarshalAs(UnmanagedType.Interface)] out object ptr);
        }
    }
}
#endif
