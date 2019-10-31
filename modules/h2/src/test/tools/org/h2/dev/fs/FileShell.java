/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.h2.command.dml.BackupCommand;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * A shell tool that allows to list and manipulate files.
 */
public class FileShell extends Tool {

    private boolean verbose;
    private BufferedReader reader;
    private PrintStream err = System.err;
    private InputStream in = System.in;
    private String currentWorkingDirectory;

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-verbose]</td>
     * <td>Print stack traces</td></tr>
     * <tr><td>[-run ...]</td>
     * <td>Execute the given commands and exit</td></tr>
     * </table>
     * Multiple commands may be executed if separated by ;
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new FileShell().runTool(args);
    }

    /**
     * Sets the standard error stream.
     *
     * @param err the new standard error stream
     */
    public void setErr(PrintStream err) {
        this.err = err;
    }

    /**
     * Redirects the standard input. By default, System.in is used.
     *
     * @param in the input stream to use
     */
    public void setIn(InputStream in) {
        this.in = in;
    }

    /**
     * Redirects the standard input. By default, System.in is used.
     *
     * @param reader the input stream reader to use
     */
    public void setInReader(BufferedReader reader) {
        this.reader = reader;
    }

    /**
     * Run the shell tool with the given command line settings.
     *
     * @param args the command line settings
     */
    @Override
    public void runTool(String... args) throws SQLException {
        try {
            currentWorkingDirectory = new File(".").getCanonicalPath();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "cwd");
        }
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-run")) {
                try {
                    execute(args[++i]);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            } else if (arg.equals("-verbose")) {
                verbose = true;
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        promptLoop();
    }

    private void promptLoop() {
        println("");
        println("Welcome to H2 File Shell " + Constants.getFullVersion());
        println("Exit with Ctrl+C");
        showHelp();
        if (reader == null) {
            reader = new BufferedReader(new InputStreamReader(in));
        }
        println(FileUtils.toRealPath(currentWorkingDirectory));
        while (true) {
            try {
                print("> ");
                String line = readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                try {
                    execute(line);
                } catch (Exception e) {
                    error(e);
                }
            } catch (Exception e) {
                error(e);
                break;
            }
        }
    }

    private void execute(String line) throws IOException {
        String[] commands = StringUtils.arraySplit(line, ';', true);
        for (String command : commands) {
            String[] list = StringUtils.arraySplit(command, ' ', true);
            if (!execute(list)) {
                break;
            }
        }
    }

    private boolean execute(String[] list) throws IOException {
        // TODO unit tests for everything (multiple commands, errors, ...)
        // TODO less (support large files)
        // TODO hex dump
        int i = 0;
        String c = list[i++];
        if ("exit".equals(c) || "quit".equals(c)) {
            end(list, i);
            return false;
        } else if ("help".equals(c) || "?".equals(c)) {
            showHelp();
            end(list, i);
        } else if ("cat".equals(c)) {
            String file = getFile(list[i++]);
            end(list, i);
            cat(file, Long.MAX_VALUE);
        } else if ("cd".equals(c)) {
            String dir = getFile(list[i++]);
            end(list, i);
            if (FileUtils.isDirectory(dir)) {
                currentWorkingDirectory = dir;
                println(dir);
            } else {
                println("Not a directory: " + dir);
            }
        } else if ("chmod".equals(c)) {
            String mode = list[i++];
            String file = getFile(list[i++]);
            end(list, i);
            if ("-w".equals(mode)) {
                boolean success = FileUtils.setReadOnly(file);
                println(success ? "Success" : "Failed");
            } else {
                println("Unsupported mode: " + mode);
            }
        } else if ("cp".equals(c)) {
            String source = getFile(list[i++]);
            String target = getFile(list[i++]);
            end(list, i);
            IOUtils.copyFiles(source, target);
        } else if ("head".equals(c)) {
            String file = getFile(list[i++]);
            end(list, i);
            cat(file, 1024);
        } else if ("ls".equals(c)) {
            String dir = currentWorkingDirectory;
            if (i < list.length) {
                dir = getFile(list[i++]);
            }
            end(list, i);
            println(dir);
            for (String file : FileUtils.newDirectoryStream(dir)) {
                StringBuilder buff = new StringBuilder();
                buff.append(FileUtils.isDirectory(file) ? "d" : "-");
                buff.append(FileUtils.canWrite(file) ? "rw" : "r-");
                buff.append(' ');
                buff.append(String.format("%10d", FileUtils.size(file)));
                buff.append(' ');
                long lastMod = FileUtils.lastModified(file);
                buff.append(new Timestamp(lastMod).toString());
                buff.append(' ');
                buff.append(FileUtils.getName(file));
                println(buff.toString());
            }
        } else if ("mkdir".equals(c)) {
            String dir = getFile(list[i++]);
            end(list, i);
            FileUtils.createDirectories(dir);
        } else if ("mv".equals(c)) {
            String source = getFile(list[i++]);
            String target = getFile(list[i++]);
            end(list, i);
            FileUtils.move(source, target);
        } else if ("pwd".equals(c)) {
            end(list, i);
            println(FileUtils.toRealPath(currentWorkingDirectory));
        } else if ("rm".equals(c)) {
            if ("-r".equals(list[i])) {
                i++;
                String dir = getFile(list[i++]);
                end(list, i);
                FileUtils.deleteRecursive(dir, true);
            } else if ("-rf".equals(list[i])) {
                i++;
                String dir = getFile(list[i++]);
                end(list, i);
                FileUtils.deleteRecursive(dir, false);
            } else {
                String file = getFile(list[i++]);
                end(list, i);
                FileUtils.delete(file);
            }
        } else if ("touch".equals(c)) {
            String file = getFile(list[i++]);
            end(list, i);
            truncate(file, FileUtils.size(file));
        } else if ("truncate".equals(c)) {
            if ("-s".equals(list[i])) {
                i++;
                long length = Long.decode(list[i++]);
                String file = getFile(list[i++]);
                end(list, i);
                truncate(file, length);
            } else {
                println("Unsupported option");
            }
        } else if ("unzip".equals(c)) {
            String file = getFile(list[i++]);
            end(list, i);
            unzip(file, currentWorkingDirectory);
        } else if ("zip".equals(c)) {
            boolean recursive = false;
            if ("-r".equals(list[i])) {
                i++;
                recursive = true;
            }
            String target = getFile(list[i++]);
            ArrayList<String> source = New.arrayList();
            readFileList(list, i, source, recursive);
            zip(target, currentWorkingDirectory, source);
        }
        return true;
    }

    private static void end(String[] list, int index) throws IOException {
        if (list.length != index) {
            throw new IOException("End of command expected, got: " + list[index]);
        }
    }

    private void cat(String fileName, long length) {
        if (!FileUtils.exists(fileName)) {
            print("No such file: " + fileName);
        }
        if (FileUtils.isDirectory(fileName)) {
            print("Is a directory: " + fileName);
        }
        InputStream inFile = null;
        try {
            inFile = FileUtils.newInputStream(fileName);
            IOUtils.copy(inFile, out, length);
        } catch (IOException e) {
            error(e);
        } finally {
            IOUtils.closeSilently(inFile);
        }
        println("");
    }

    private void truncate(String fileName, long length) {
        FileChannel f = null;
        try {
            f = FileUtils.open(fileName, "rw");
            f.truncate(length);
        } catch (IOException e) {
            error(e);
        } finally {
            try {
                f.close();
            } catch (IOException e) {
                error(e);
            }
        }
    }

    private void error(Exception e) {
        println("Exception: " + e.getMessage());
        if (verbose) {
            e.printStackTrace(err);
        }
    }

    private static void zip(String zipFileName, String base,
            ArrayList<String> source) {
        FileUtils.delete(zipFileName);
        OutputStream fileOut = null;
        try {
            fileOut = FileUtils.newOutputStream(zipFileName, false);
            ZipOutputStream zipOut = new ZipOutputStream(fileOut);
            for (String fileName : source) {
                String f = FileUtils.toRealPath(fileName);
                if (!f.startsWith(base)) {
                    DbException.throwInternalError(f + " does not start with " + base);
                }
                if (f.endsWith(zipFileName)) {
                    continue;
                }
                if (FileUtils.isDirectory(fileName)) {
                    continue;
                }
                f = f.substring(base.length());
                f = BackupCommand.correctFileName(f);
                ZipEntry entry = new ZipEntry(f);
                zipOut.putNextEntry(entry);
                InputStream in = null;
                try {
                    in = FileUtils.newInputStream(fileName);
                    IOUtils.copyAndCloseInput(in, zipOut);
                } catch (FileNotFoundException e) {
                    // the file could have been deleted in the meantime
                    // ignore this (in this case an empty file is created)
                } finally {
                    IOUtils.closeSilently(in);
                }
                zipOut.closeEntry();
            }
            zipOut.closeEntry();
            zipOut.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, zipFileName);
        } finally {
            IOUtils.closeSilently(fileOut);
        }
    }

    private void unzip(String zipFileName, String targetDir) {
        InputStream inFile = null;
        try {
            inFile = FileUtils.newInputStream(zipFileName);
            ZipInputStream zipIn = new ZipInputStream(inFile);
            while (true) {
                ZipEntry entry = zipIn.getNextEntry();
                if (entry == null) {
                    break;
                }
                String fileName = entry.getName();
                // restoring windows backups on linux and vice versa
                fileName = fileName.replace('\\',
                        SysProperties.FILE_SEPARATOR.charAt(0));
                fileName = fileName.replace('/',
                        SysProperties.FILE_SEPARATOR.charAt(0));
                if (fileName.startsWith(SysProperties.FILE_SEPARATOR)) {
                    fileName = fileName.substring(1);
                }
                OutputStream o = null;
                try {
                    o = FileUtils.newOutputStream(targetDir
                            + SysProperties.FILE_SEPARATOR + fileName, false);
                    IOUtils.copy(zipIn, o);
                    o.close();
                } finally {
                    IOUtils.closeSilently(o);
                }
                zipIn.closeEntry();
            }
            zipIn.closeEntry();
            zipIn.close();
        } catch (IOException e) {
            error(e);
        } finally {
            IOUtils.closeSilently(inFile);
        }
    }

    private int readFileList(String[] list, int i, ArrayList<String> target,
            boolean recursive) throws IOException {
        while (i < list.length) {
            String c = list[i++];
            if (";".equals(c)) {
                break;
            }
            c = getFile(c);
            if (!FileUtils.exists(c)) {
                throw new IOException("File not found: " + c);
            }
            if (recursive) {
                addFilesRecursive(c, target);
            } else {
                target.add(c);
            }
        }
        return i;
    }

    private void addFilesRecursive(String f, ArrayList<String> target) {
        if (FileUtils.isDirectory(f)) {
            for (String c : FileUtils.newDirectoryStream(f)) {
                addFilesRecursive(c, target);
            }
        } else {
            target.add(getFile(f));
        }
    }

    private String getFile(String f) {
        if (FileUtils.isAbsolute(f)) {
            return f;
        }
        String unwrapped = FileUtils.unwrap(f);
        String prefix = f.substring(0, f.length() - unwrapped.length());
        f = prefix + currentWorkingDirectory + SysProperties.FILE_SEPARATOR + unwrapped;
        return FileUtils.toRealPath(f);
    }

    private void showHelp() {
        println("Commands are case sensitive");
        println("? or help                  " +
                "Display this help");
        println("cat <file>                 " +
                "Print the contents of the file");
        println("cd <dir>                   " +
                "Change the directory");
        println("chmod -w <file>            " +
                "Make the file read-only");
        println("cp <source> <target>       " +
                "Copy a file");
        println("head <file>                " +
                "Print the first few lines of the contents");
        println("ls [<dir>]                 " +
                "Print the directory contents");
        println("mkdir <dir>                " +
                "Create a directory (including parent directories)");
        println("mv <source> <target>       " +
                "Rename a file or directory");
        println("pwd                        " +
                "Print the current working directory");
        println("rm <file>                  " +
                "Remove a file");
        println("rm -r <dir>                " +
                "Remove a directory, recursively");
        println("rm -rf <dir>               " +
                "Remove a directory, recursively; force");
        println("touch <file>               " +
                "Update the last modified date (creates the file)");
        println("truncate -s <size> <file>  " +
                "Set the file length");
        println("unzip <zip>                " +
                "Extract all files from the zip file");
        println("zip [-r] <zip> <files..>   " +
                "Create a zip file (-r to recurse directories)");
        println("exit                       Exit");
        println("");
    }

    private String readLine() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Aborted");
        }
        return line;
    }

    private void print(String s) {
        out.print(s);
        out.flush();
    }

    private void println(String s) {
        out.println(s);
        out.flush();
    }

}
