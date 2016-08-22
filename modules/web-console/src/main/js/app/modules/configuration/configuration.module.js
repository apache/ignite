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

import GeneratorXml from './generator/Xml.service';
import GeneratorJava from './generator/Java.service';
import GeneratorDocker from './generator/Docker.service';
import GeneratorPom from './generator/Pom.service';

import igniteSidebarDirective from './sidebar.directive';

// Ignite events groups.
angular
.module('ignite-console.configuration', [

])
.provider(...igniteEventGroups)
.provider(...igniteSidebar)
.directive(...igniteSidebarDirective)
.service(...GeneratorXml)
.service(...GeneratorJava)
.service(...GeneratorDocker)
.service(...GeneratorPom);
