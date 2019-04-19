/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Service
{
    using System.Collections;
    using System.ComponentModel;
    using System.Configuration.Install;
    using System.ServiceProcess;

    /// <summary>
    /// Service installer for InstallUtil.
    /// </summary>
    [RunInstaller(true)]
    public class IgniteServiceInstaller : Installer
    {
        /// <summary>
        /// Gets or sets the service arguments.
        /// </summary>
        public static string Args { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteServiceInstaller"/> class.
        /// </summary>
        public IgniteServiceInstaller()
        {
            Installers.Add(new ServiceInstaller
            {
                StartType = ServiceStartMode.Automatic,
                ServiceName = IgniteService.SvcName,
                Description = IgniteService.SvcDesc,
                DisplayName = IgniteService.SvcDisplayName
            });

            Installers.Add(new ServiceProcessInstaller {Account = ServiceAccount.LocalSystem});
        }

        /** <inheritdoc /> */
        protected override void OnBeforeInstall(IDictionary savedState)
        {
            if (!string.IsNullOrWhiteSpace(Args))
            {
                Context.Parameters["assemblyPath"] =
                    string.Format("\"{0}\" {1}", Context.Parameters["assemblyPath"], Args);
            }

            base.OnBeforeInstall(savedState);
        }
    }
}
