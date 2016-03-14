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

package org.apache.ignite.scalar.examples.igfs

import java.io._
import java.util.{Collection => JavaCollection, Collections, Comparator, HashSet => JavaHashSet, List => JavaList, TreeSet => JavaTreeSet}

import org.apache.ignite.compute.ComputeJobResult
import org.apache.ignite.examples.igfs.IgfsNodeStartup
import org.apache.ignite.igfs.IgfsPath
import org.apache.ignite.igfs.mapreduce._
import org.apache.ignite.igfs.mapreduce.records.IgfsNewLineRecordResolver
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{IgniteException, IgniteFileSystem}

import scala.collection.JavaConversions._

/**
 * Example that shows how to use [[IgfsTask]] to find lines matching particular pattern in the file in pretty
 * the same way as `grep` command does.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * IGFS: `'ignite.sh examples/config/filesystem/example-igfs.xml'`.
 * <p>
 * Alternatively you can run [[IgfsNodeStartup]] in another JVM which will start
 * node with `examples/config/filesystem/example-igfs.xml` configuration.
 */
object ScalarIgfsMapReduceExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/filesystem/example-igfs.xml"

    if (args.length == 0)
        println("Please provide file name and regular expression.")
    else if (args.length == 1)
        println("Please provide regular expression.")
    else {
        scalar(CONFIG) {
            println()
            println(">>> IGFS map reduce example started.")

            val fileName = args(0)

            val file = new File(fileName)

            val regexStr = args(1)

            val fs = ignite$.fileSystem("igfs")

            val workDir = new IgfsPath("/examples/fs")
            val fsPath = new IgfsPath(workDir, file.getName)

            writeFile(fs, fsPath, file)

            val lines: JavaCollection[Line] =
                fs.execute(new GrepTask, IgfsNewLineRecordResolver.NEW_LINE, Collections.singleton(fsPath), regexStr)

            if (lines.isEmpty) {
                println()
                println("No lines were found.")
            }
            else {
                for (line <- lines) print(line.fileLine())
            }
        }
    }

    /**
     * Write file to the Ignite file system.
     *
     * @param fs Ignite file system.
     * @param fsPath Ignite file system path.
     * @param file File to write.
     * @throws Exception In case of exception.
     */
    @throws(classOf[Exception])
    private def writeFile(fs: IgniteFileSystem, fsPath: IgfsPath, file: File) {
        println()
        println("Copying file to IGFS: " + file)

        val os = fs.create(fsPath, true)
        val fis = new FileInputStream(file)

        try {
            val buf = new Array[Byte](2048)

            var read = fis.read(buf)

            while (read != -1) {
                os.write(buf, 0, read)

                read = fis.read(buf)
            }
        }
        finally {
            U.closeQuiet(os)
            U.closeQuiet(fis)
        }
    }

    /**
     * Print particular string.
     *
     * @param str String.
     */
    private def print(str: String) {
        println(">>> " + str)
    }
}

/**
 * Grep task.
 */
private class GrepTask extends IgfsTask[String, JavaCollection[Line]] {
    def createJob(path: IgfsPath, range: IgfsFileRange, args: IgfsTaskArgs[String]): IgfsJob = {
        new GrepJob(args.userArgument())
    }

    override def reduce(results: JavaList[ComputeJobResult]): JavaCollection[Line] = {
        val lines: JavaCollection[Line] = new JavaTreeSet[Line](new Comparator[Line] {
            def compare(line1: Line, line2: Line): Int = {
                if (line1.rangePosition < line2.rangePosition)
                    -1
                else if (line1.rangePosition > line2.rangePosition)
                    1
                else
                    line1.lineIndex - line2.lineIndex
            }
        })

        for (res <- results) {
            if (res.getException != null)
                throw res.getException
            val line = res.getData[JavaCollection[Line]]

            if (line != null)
                lines.addAll(line)
        }

        lines
    }
}

/**
 * Grep job.
 *
 * @param regex Regex string.
 */
private class GrepJob(private val regex: String) extends IgfsInputStreamJobAdapter {
    @throws(classOf[IgniteException])
    @throws(classOf[IOException])
    def execute(igfs: IgniteFileSystem, in: IgfsRangeInputStream): AnyRef = {
        var res: JavaCollection[Line] = null

        val start = in.startOffset

        val br = new BufferedReader(new InputStreamReader(in))

        try {
            var ctr = 0

            var line = br.readLine

            while (line != null) {
                if (line.matches(".*" + regex + ".*")) {
                    if (res == null)
                        res = new JavaHashSet[Line]

                    res.add(new Line(start, ctr, line))

                    ctr += 1
                }

                line = br.readLine
            }
        }
        finally {
            U.closeQuiet(br)
        }

        res
    }
}

/**
 * Single file line with it's position.
 * 
 * @param rangePos Line start position in the file.
 * @param lineIdx Matching line index within the range.
 * @param line File line.
 */
private class Line(private val rangePos: Long, private val lineIdx: Int, private val line: String) {
    /**
     * @return Range position.
     */
    def rangePosition(): Long = {
        rangePos
    }

    /**
     * @return Matching line index within the range.
     */
    def lineIndex(): Int = {
        lineIdx
    }

    /**
     * @return File line.
     */
    def fileLine(): String = {
        line
    }
}
