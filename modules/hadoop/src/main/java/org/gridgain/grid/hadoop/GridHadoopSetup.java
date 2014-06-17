/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

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
     * This operation prepares the clean unpacked Hadoop distributive to work as client with GridGain-Hadoop.
     * It performs these operations:
     * <ul>
     *     <li>Check for setting of HADOOP_HOME environment variable.</li>
     *     <li>Try to resolve HADOOP_COMMON_HOME or evaluate it relative to HADOOP_HOME.</li>
     *     <li>In Windows check if winutils.exe exists and try to fix issue with some restrictions.</li>
     *     <li>In Windows check new line character issues in CMD scripts.</li>
     *     <li>Scan Hadoop lib directory to detect GridGain JARs. If these don't exist tries to create ones.</li>
     * </ul>
     * @throws IOException If fails.
     */
    private static void configureHadoop() throws IOException {
        String hadoopHome = System.getenv("HADOOP_HOME");

        if (hadoopHome == null || hadoopHome.isEmpty()) {
            System.out.println("ERROR: HADOOP_HOME variable is not set. Please set HADOOP_HOME to valid Hadoop " +
                "installation folder and run setup tool again.");

            return;
        }

        hadoopHome = hadoopHome.replaceAll("\"", "");

        System.out.println("\nFound Hadoop in " + hadoopHome + "\n");

        String hadoopCommonHome = System.getenv("HADOOP_COMMON_HOME");

        File hadoopDir = new File(hadoopHome);

        File hadoopCommonDir;

        if (hadoopCommonHome == null || hadoopCommonHome.isEmpty())
            hadoopCommonDir = new File(hadoopDir, "share/hadoop/common");
        else
            hadoopCommonDir = new File(hadoopCommonHome);

        File hadoopCommonLibDir = new File(hadoopCommonDir, "lib");

        if (U.isWindows()) {
            File hadoopBinDir = new File(hadoopDir, "bin");

            if (!hadoopBinDir.exists()) {
                System.out.println("Invalid Hadoop home directory. 'bin' subdirectory was not found.");

                return;
            }

            File winutilsFile = new File(hadoopBinDir, WINUTILS_EXE);

            if (!winutilsFile.exists() && getAnswer("File " + WINUTILS_EXE + " doesn't exist. " +
                "It may be replaced by a stub. Create it?"))
                winutilsFile.createNewFile();

            processCmdFiles(hadoopDir, "bin", "sbin", "libexec");
        }

        String gridgainHome = U.getGridGainHome();

        System.out.println("GRIDGAIN_HOME=" + gridgainHome);

        File gridGainLibs = new File(new File(gridgainHome), "libs");

        File[] jarFiles = gridGainLibs.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });

        boolean jarsExist = true;

        for (File file : jarFiles) {
            File targetFile = new File(hadoopCommonLibDir, file.getName());

            if (!targetFile.exists())
                jarsExist = false;
        }

        if (!jarsExist && getAnswer("GridGain JAR files are not found in Hadoop libs directory. " +
            "Create appropriate symbolic links?")) {
            File[] oldGridGainJarFiles = hadoopCommonLibDir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.startsWith("gridgain-");
                }
            });

            if (oldGridGainJarFiles.length > 0) {
                if (!getAnswer("The Hadoop libs directory contains JARs from other GridGain installation. " +
                    "It needs to be deleted to continue. Continue?"))
                    return;

                for (File file : oldGridGainJarFiles)
                    file.delete();
            }

            for (File file : jarFiles) {
                File targetFile = new File(hadoopCommonLibDir, file.getName());

                Files.createSymbolicLink(targetFile.toPath(), file.toPath());
            }
        }

        System.out.println("Hadoop setting is completed.");
    }

    /**
     * Writes the question end read the boolean answer from the console.
     *
     * @param question Question to write.
     * @return {@code true} if user inputs 'Y' or 'y', {@code false} otherwise.
     * @throws IOException If fails.
     */
    private static boolean getAnswer(String question) throws IOException {
        System.out.print(question + " (Y/N):");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String answer = br.readLine();

        if (answer != null)
            if ("Y".equals(answer.toUpperCase().trim()))
                return true;

        return false;
    }

    /**
     * Checks that CMD files have valid MS Windows new line characters. If not, writes question to console and reads the
     * answer. If it's 'Y' then backups original files and corrects invalid new line characters.
     *
     * @param rootDir Root directory to process.
     * @param dirs Directories inside of the root to process.
     * @throws IOException If fails.
     */
    private static void processCmdFiles(File rootDir, String... dirs) throws IOException {
        Boolean answer = false;

        for (String dir : dirs) {
            File subDir = new File(rootDir, dir);

            File[] cmdFiles = subDir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".cmd");
                }
            });

            for (File file : cmdFiles) {
                String content;

                try (Scanner scanner = new Scanner(file)) {
                    content = scanner.useDelimiter("\\Z").next();
                }

                boolean invalid = false;

                for (int i = 0; i < content.length(); i++) {
                    if (content.charAt(i) == '\n' && (i == 0 || content.charAt(i - 1) != '\r')) {
                        invalid = true;

                        break;
                    }
                }

                if (invalid) {
                    answer = answer || getAnswer("One or more *.CMD files has invalid new line character. Replace them?");

                    if (!answer)
                        return;

                    file.renameTo(new File(file.getAbsolutePath() + ".bak"));

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                        for (int i = 0; i < content.length(); i++) {
                            if (content.charAt(i) == '\n' && (i == 0 || content.charAt(i - 1) != '\r'))
                                writer.write("\r");

                            writer.write(content.charAt(i));
                        }
                    }
                }
            }
        }
    }
}
