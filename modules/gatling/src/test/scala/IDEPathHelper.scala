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

import java.nio.file.Paths

/** */
object IDEPathHelper {
	private val projectRootDir = Paths.get(getClass.getClassLoader.getResource("gatling.conf").toURI).getParent.getParent.getParent
	private val mavenTargetDirectory = projectRootDir.resolve("target")
	private val mavenSrcTestDirectory = projectRootDir.resolve("src").resolve("test")

	val mavenSourcesDirectory = mavenSrcTestDirectory.resolve("scala")
	val mavenResourcesDirectory = mavenSrcTestDirectory.resolve("resources")
	val mavenBinariesDirectory = mavenTargetDirectory.resolve("test-classes")
	val resultsDirectory = mavenTargetDirectory.resolve("gatling")
}
