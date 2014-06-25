/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Setup tool to configure Hadoop client.
 */
public class GridHadoopSetup {
    /** */
    public static final String WINUTILS_EXE = "winutils.exe";

    /**
     * The main method.
     * @param ignore Params.
     * @throws IOException If fails.
     */
    public static void main(String[] ignore) throws IOException {
        //Ignore arguments. The only supported operation runs by default.
        configureHadoop();
    }

    /**
     * Exit with message.
     *
     * @param msg Exit message.
     */
    private static void exit(String msg) {
        X.println("### " + msg);
        X.println("### Setup failed, exiting... ");

        System.exit(1);
    }

    /**
     * Prints message.
     *
     * @param msg Message.
     */
    private static void println(String msg) {
        X.println(">>> " + msg);
    }

    /**
     * This operation prepares the clean unpacked Hadoop distributive to work as client with GridGain-Hadoop.
     * It performs these operations:
     * <ul>
     *     <li>Check for setting of HADOOP_HOME environment variable.</li>
     *     <li>Try to resolve HADOOP_COMMON_HOME or evaluate it relative to HADOOP_HOME.</li>
     *     <li>In Windows check if winutils.exe exists and try to fix issue with some restrictions.</li>
     *     <li>In Windows check new line character issues in CMD scripts.</li>
     *     <li>Scan Hadoop lib directory to detect GridGain JARs. If these don't exist tries to create ones.</li>
     * </ul>
     */
    private static void configureHadoop() {
        String gridgainHome = U.getGridGainHome();

        println("GRIDGAIN_HOME is set to '" + gridgainHome + "'.");

        String hadoopHome = System.getenv("HADOOP_HOME");

        if (hadoopHome == null || hadoopHome.isEmpty())
            exit("HADOOP_HOME environment variable is not set. Please set HADOOP_HOME to " +
                "valid Hadoop installation folder and run setup tool again.");

        hadoopHome = hadoopHome.replaceAll("\"", "");

        println("HADOOP_HOME is set to '" + hadoopHome + "'.");

        File hadoopDir = new File(hadoopHome);
        
        if (!hadoopDir.exists())
            exit("Hadoop installation folder does not exist.");

        if (!hadoopDir.isDirectory())
            exit("HADOOP_HOME must point to a directory.");

        if (!hadoopDir.canRead())
            exit("Hadoop installation folder can not be read. Please check permissions.");

        File hadoopCommonDir;

        String hadoopCommonHome = System.getenv("HADOOP_COMMON_HOME");

        if (F.isEmpty(hadoopCommonHome)) {
            hadoopCommonDir = new File(hadoopDir, "share/hadoop/common");

            println("HADOOP_COMMON_HOME is not set, will use '" + hadoopCommonDir.getPath() + "'.");
        }
        else {
            println("HADOOP_COMMON_HOME is set to '" + hadoopCommonHome + "'.");

            hadoopCommonDir = new File(hadoopCommonHome);
        }

        if (!hadoopCommonDir.canRead())
            exit("Failed to read Hadoop common dir in '" + hadoopCommonHome + "'.");

        File hadoopCommonLibDir = new File(hadoopCommonDir, "lib");

        if (!hadoopCommonLibDir.canRead())
            exit("Failed to read Hadoop 'lib' folder in '" + hadoopCommonLibDir.getPath() + "'.");

        if (U.isWindows()) {
            File hadoopBinDir = new File(hadoopDir, "bin");

            if (!hadoopBinDir.canRead())
                exit("Failed to read subdirectory 'bin' in HADOOP_HOME.");

            File winutilsFile = new File(hadoopBinDir, WINUTILS_EXE);

            if (!winutilsFile.exists() && ask("File " + WINUTILS_EXE + " does not exist. " +
                "It may be replaced by a stub. Create it?")) {
                println("Creating file stub '" + winutilsFile.getAbsolutePath() + "'.");

                boolean ok = false;

                try {
                    ok = winutilsFile.createNewFile();
                }
                catch (IOException ignore) {
                    // No-op.
                }

                if (!ok)
                    exit("Failed to create '" + WINUTILS_EXE + "' file. Please check permissions.");
            }

            processCmdFiles(hadoopDir, "bin", "sbin", "libexec");
        }

        File gridGainLibs = new File(new File(gridgainHome), "libs");

        if (!gridGainLibs.exists())
            exit("GridGain 'libs' folder is not found.");

        File[] jarFiles = gridGainLibs.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });

        boolean jarsExist = true;

        for (File file : jarFiles) {
            File targetFile = new File(hadoopCommonLibDir, file.getName());

            if (!linkTargetExists(targetFile) || !targetFile.exists()) {
                jarsExist = false;

                break;
            }
        }

        if (!jarsExist && ask("GridGain JAR files are not found in Hadoop 'lib' directory. " +
            "Create appropriate symbolic links?")) {
            File[] oldGridGainJarFiles = hadoopCommonLibDir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.startsWith("gridgain-");
                }
            });

            if (oldGridGainJarFiles.length > 0) {
                if (!ask("The Hadoop 'lib' directory contains JARs from other GridGain installation. " +
                    "They must be deleted to continue. Continue?"))
                    return;

                for (File file : oldGridGainJarFiles) {
                    println("Deleting '" + file.getAbsolutePath() + "'.");

                    if (!file.delete())
                        exit("Failed to delete file '" + file.getPath() + "'.");
                }
            }

            for (File file : jarFiles) {
                File targetFile = new File(hadoopCommonLibDir, file.getName());

                println("Creating symbolic link '" + targetFile.getAbsolutePath() + "'.");

                try {
                    Files.createSymbolicLink(targetFile.toPath(), file.toPath());
                }
                catch (IOException e) {
                    exit("Failed to create symbolic link '" + targetFile.getPath() + "'. Please check permissions.");
                }
            }
        }

        println("Hadoop setup is complete.");
    }

    /**
     * Checks if link target exists.
     *
     * @param link Symbolic link.
     * @return {@code true} If link target exists.
     */
    private static boolean linkTargetExists(File link) {
        if (!Files.isSymbolicLink(link.toPath()))
            return true; // It is a real file or it does not exist.

        Path target = null;

        try {
            target = Files.readSymbolicLink(link.toPath());
        }
        catch (IOException e) {
            exit("Failed to read symbolic link: " + link.getAbsolutePath());
        }

        if (Files.notExists(target)) {
            println("Found link pointing to non-existing location: " + link.getAbsolutePath() + " -> " +
                target.toAbsolutePath());

            return false;
        }

        return true;
    }

    /**
     * Writes the question end read the boolean answer from the console.
     *
     * @param question Question to write.
     * @return {@code true} if user inputs 'Y' or 'y', {@code false} otherwise.
     */
    private static boolean ask(String question) {
        X.print("  < " + question + " (Y/N): ");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String answer = null;

        try {
            answer = br.readLine();
        }
        catch (IOException e) {
            exit("Failed to read answer: " + e.getMessage());
        }

        if (answer != null && "Y".equals(answer.toUpperCase().trim())) {
            X.println("  > Yes.");

            return true;
        }
        else {
            X.println("  > No.");

            return false;
        }
    }

    /**
     * Checks that CMD files have valid MS Windows new line characters. If not, writes question to console and reads the
     * answer. If it's 'Y' then backups original files and corrects invalid new line characters.
     *
     * @param rootDir Root directory to process.
     * @param dirs Directories inside of the root to process.
     */
    private static void processCmdFiles(File rootDir, String... dirs) {
        Boolean answer = false;

        for (String dir : dirs) {
            File subDir = new File(rootDir, dir);

            File[] cmdFiles = subDir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".cmd");
                }
            });

            for (File file : cmdFiles) {
                String content = null;

                try (Scanner scanner = new Scanner(file)) {
                    content = scanner.useDelimiter("\\Z").next();
                }
                catch (FileNotFoundException e) {
                    exit("Failed to read file '" + file + "'.");
                }

                boolean invalid = false;

                for (int i = 0; i < content.length(); i++) {
                    if (content.charAt(i) == '\n' && (i == 0 || content.charAt(i - 1) != '\r')) {
                        invalid = true;

                        break;
                    }
                }

                if (invalid) {
                    answer = answer || ask("One or more *.CMD files has invalid new line character. Replace them?");

                    if (!answer)
                        return;

                    if (!file.renameTo(new File(file.getAbsolutePath() + ".bak")))
                        exit("Failed to rename file '" + file.getPath() + "'.");

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                        for (int i = 0; i < content.length(); i++) {
                            if (content.charAt(i) == '\n' && (i == 0 || content.charAt(i - 1) != '\r'))
                                writer.write("\r");

                            writer.write(content.charAt(i));
                        }
                    }
                    catch (IOException e) {
                        exit("Failed to write file '" + file.getPath() + "': " + e.getMessage());
                    }
                }
            }
        }
    }
}
