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

import angular from 'angular';


import igniteSidebar from './Sidebar.provider';
import IgniteVersion from './Version.service';

import IgniteClusterDefaults from './generator/defaults/Cluster.service';
import IgniteClusterPlatformDefaults from './generator/defaults/Cluster.platform.service';
import IgniteCacheDefaults from './generator/defaults/Cache.service';
import IgniteCachePlatformDefaults from './generator/defaults/Cache.platform.service';
import IgniteIGFSDefaults from './generator/defaults/IGFS.service';
import IgniteEventGroups from './generator/defaults/Event-groups.service';

import IgniteConfigurationGenerator from './generator/ConfigurationGenerator';
import IgnitePlatformGenerator from './generator/PlatformGenerator';

import IgniteSpringTransformer from './generator/SpringTransformer.service';
import IgniteJavaTransformer from './generator/JavaTransformer.service';
import SharpTransformer from './generator/SharpTransformer.service';
import IgniteDockerGenerator from './generator/Docker.service';
import IgniteMavenGenerator from './generator/Maven.service';
import IgniteGeneratorProperties from './generator/Properties.service';
import IgniteReadmeGenerator from './generator/Readme.service';
import IgniteCustomGenerator from './generator/Custom.service';

import igniteSidebarDirective from './sidebar.directive';

// Ignite events groups.
angular
.module('ignite-console.configuration', [

])
.provider(...igniteSidebar)
.directive(...igniteSidebarDirective)
.service('IgniteConfigurationGenerator', () => IgniteConfigurationGenerator)
.service('IgnitePlatformGenerator', IgnitePlatformGenerator)
.service('SpringTransformer', () => IgniteSpringTransformer)
.service('JavaTransformer', () => IgniteJavaTransformer)
.service('IgniteSharpTransformer', SharpTransformer)
.service('IgniteVersion', IgniteVersion)
.service('IgniteEventGroups', IgniteEventGroups)
.service('IgniteClusterDefaults', IgniteClusterDefaults)
.service('IgniteClusterPlatformDefaults', IgniteClusterPlatformDefaults)
.service('IgniteCacheDefaults', IgniteCacheDefaults)
.service('IgniteCachePlatformDefaults', IgniteCachePlatformDefaults)
.service('IgniteIGFSDefaults', IgniteIGFSDefaults)
.service('IgnitePropertiesGenerator', IgniteGeneratorProperties)
.service('IgniteReadmeGenerator', IgniteReadmeGenerator)
.service('IgniteDockerGenerator', IgniteDockerGenerator)
.service('IgniteMavenGenerator', IgniteMavenGenerator)
.service('IgniteCustomGenerator', IgniteCustomGenerator);
