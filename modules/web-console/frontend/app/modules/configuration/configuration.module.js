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

import igniteEventGroups from './EventGroups.provider';
import igniteSidebar from './Sidebar.provider';
import Version from './Version.service';

import clusterDefaults from './generator/defaults/cluster.provider';
import clusterPlatformDefaults from './generator/defaults/cluster.platform.provider';
import cacheDefaults from './generator/defaults/cache.provider';
import cachePlatformDefaults from './generator/defaults/cache.platform.provider';
import igfsDefaults from './generator/defaults/igfs.provider';

import ConfigurationGenerator from './generator/ConfigurationGenerator';
import PlatformGenerator from './generator/PlatformGenerator';

import SpringTransformer from './generator/SpringTransformer.service';
import JavaTransformer from './generator/JavaTransformer.service';
import SharpTransformer from './generator/SharpTransformer.service';
import GeneratorDocker from './generator/Docker.service';
import GeneratorPom from './generator/Pom.service';
import GeneratorProperties from './generator/Properties.service';
import GeneratorReadme from './generator/Readme.service';

import igniteSidebarDirective from './sidebar.directive';

// Ignite events groups.
angular
.module('ignite-console.configuration', [

])
.provider('igniteClusterDefaults', clusterDefaults)
.provider('igniteClusterPlatformDefaults', clusterPlatformDefaults)
.provider('igniteCacheDefaults', cacheDefaults)
.provider('igniteCachePlatformDefaults', cachePlatformDefaults)
.provider('igniteIgfsDefaults', igfsDefaults)
.provider(...igniteEventGroups)
.provider(...igniteSidebar)
.directive(...igniteSidebarDirective)
.service('IgniteVersion', Version)
.service('IgniteConfigurationGenerator', ConfigurationGenerator)
.service('IgnitePlatformGenerator', PlatformGenerator)
.service('SpringTransformer', SpringTransformer)
.service('JavaTransformer', JavaTransformer)
.service('IgniteSharpTransformer', SharpTransformer)
.service('IgnitePropertiesGenerator', GeneratorProperties)
.service('IgniteReadmeGenerator', GeneratorReadme)
.service(...GeneratorDocker)
.service(...GeneratorPom);
