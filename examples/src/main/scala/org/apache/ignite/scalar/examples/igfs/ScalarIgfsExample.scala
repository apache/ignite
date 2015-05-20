/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.igfs

import org.apache.ignite.examples.igfs.IgfsNodeStartup
import org.apache.ignite.igfs.{IgfsException, IgfsPath}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{IgniteException, IgniteFileSystem}

import org.jetbrains.annotations.Nullable

import java.io.IOException
import java.util.{Collection => JavaCollection}

import scala.collection.JavaConversions._

/**
 * Example that shows usage of [[org.apache.ignite.IgniteFileSystem]] API. It starts a node with `IgniteFs`
 * configured and performs several file system operations (create, write, append, read and delete
 * files, create, list and delete directories).
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * IGFS: `'ignite.sh examples/config/filesystem/example-igfs.xml'`.
 * <p>
 * Alternatively you can run [[IgfsNodeStartup]] in another JVM which will start
 * node with `examples/config/filesystem/example-igfs.xml` configuration.
 */
object ScalarIgfsExample extends App  {
    /** Configuration file name. */
    private val CONFIG = "examples/config/filesystem/example-igfs.xml"

    scalar(CONFIG) {
        println()
        println(">>> IGFS example started.")

        val fs = ignite$.fileSystem("igfs")

        val workDir = new IgfsPath("/examples/fs")

        delete(fs, workDir)

        mkdirs(fs, workDir)

        printInfo(fs, workDir)

        val filePath = new IgfsPath(workDir, "file.txt")

        create(fs, filePath, Array[Byte](1, 2, 3))

        printInfo(fs, filePath)

        append(fs, filePath, Array[Byte](4, 5))

        printInfo(fs, filePath)

        read(fs, filePath)

        delete(fs, filePath)

        printInfo(fs, filePath)

        for (i <- 0 until 5)
            create(fs, new IgfsPath(workDir, "file-" + i + ".txt"), null)

        list(fs, workDir)
    }
    
    /**
     * Deletes file or directory. If directory
     * is not empty, it's deleted recursively.
     *
     * @param fs IGFS.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
    @throws(classOf[IgniteException])
    private def delete(fs: IgniteFileSystem, path: IgfsPath) {
        assert(fs != null)
        assert(path != null)

        if (fs.exists(path)) {
            val isFile = fs.info(path).isFile

            try {
                fs.delete(path, true)

                println()
                println(">>> Deleted " + (if (isFile) "file" else "directory") + ": " + path)
            }
            catch {
                case e: IgfsException =>
                    println()
                    println(">>> Failed to delete " + (if (isFile) "file" else "directory") + " [path=" + path + ", msg=" + e.getMessage + ']')
            }
        }
        else {
            println()
            println(">>> Won't delete file or directory (doesn't exist): " + path)
        }
    }

    /**
     * Creates directories.
     *
     * @param fs IGFS.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
    @throws(classOf[IgniteException])
    private def mkdirs(fs: IgniteFileSystem, path: IgfsPath) {
        assert(fs != null)
        assert(path != null)

        try {
            fs.mkdirs(path)

            println()
            println(">>> Created directory: " + path)
        }
        catch {
            case e: IgfsException =>
                println()
                println(">>> Failed to create a directory [path=" + path + ", msg=" + e.getMessage + ']')
        }

        println()
    }

    /**
     * Creates file and writes provided data to it.
     *
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
    @throws(classOf[IgniteException])
    @throws(classOf[IOException])
    private def create(fs: IgniteFileSystem, path: IgfsPath, @Nullable data: Array[Byte]) {
        assert(fs != null)
        assert(path != null)

        val out = fs.create(path, true)

        try {
            println()
            println(">>> Created file: " + path)

            if (data != null) {
                out.write(data)

                println()
                println(">>> Wrote data to file: " + path)
            }
        }
        finally {
            if (out != null) out.close()
        }

        println()
    }

    /**
     * Opens file and appends provided data to it.
     *
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
    @throws(classOf[IgniteException])
    @throws(classOf[IOException])
    private def append(fs: IgniteFileSystem, path: IgfsPath, data: Array[Byte]) {
        assert(fs != null)
        assert(path != null)
        assert(data != null)
        assert(fs.info(path).isFile)

        val out = fs.append(path, true)

        try {
            println()
            println(">>> Opened file: " + path)

            out.write(data)
        }
        finally {
            if (out != null) out.close()
        }

        println()
        println(">>> Appended data to file: " + path)
    }

    /**
     * Opens file and reads it to byte array.
     *
     * @param fs IgniteFs.
     * @param path File path.
     * @throws IgniteException If file can't be opened.
     * @throws IOException If data can't be read.
     */
    @throws(classOf[IgniteException])
    @throws(classOf[IOException])
    private def read(fs: IgniteFileSystem, path: IgfsPath) {
        assert(fs != null)
        assert(path != null)
        assert(fs.info(path).isFile)

        val data = new Array[Byte](fs.info(path).length.toInt)

        val in = fs.open(path)

        try {
            in.read(data)
        }
        finally {
            if (in != null) in.close()
        }

        println()
        println(">>> Read data from " + path + ": " + data.mkString("[", ", ", "]"))
    }

    /**
     * Lists files in directory.
     *
     * @param fs IGFS.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
    @throws(classOf[IgniteException])
    private def list(fs: IgniteFileSystem, path: IgfsPath) {
        assert(fs != null)
        assert(path != null)
        assert(fs.info(path).isDirectory)

        val files: JavaCollection[IgfsPath] = fs.listPaths(path)

        if (files.isEmpty) {
            println()
            println(">>> No files in directory: " + path)
        }
        else {
            println()
            println(">>> List of files in directory: " + path)

            for (f <- files) println(">>>     " + f.name)
        }
        println()
    }

    /**
     * Prints information for file or directory.
     *
     * @param fs IGFS.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
    @throws(classOf[IgniteException])
    private def printInfo(fs: IgniteFileSystem, path: IgfsPath) {
        println()
        println("Information for " + path + ": " + fs.info(path))
    }
}
