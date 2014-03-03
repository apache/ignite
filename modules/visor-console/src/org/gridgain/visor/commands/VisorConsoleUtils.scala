/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands

/**
 * Object with common utility functions.
 */
object VisorConsoleUtils {
    /**
     * Tests whether or not this task has specified substring in its name.
     *
     * @param taskName Task name to check.
     * @param taskClsName Task class name to check.
     * @param s Substring to check.
     */
    def containsInTaskName(taskName: String, taskClsName: String, s: String): Boolean = {
        assert(taskName != null)
        assert(taskClsName != null)

        (
            if (taskName == taskClsName) {
                val idx = taskName.lastIndexOf('.')

                if (idx >= 0) taskName.substring(idx + 1) else taskName
            }
            else
                taskName
            ).toLowerCase.contains(s)
    }
}
