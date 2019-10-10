/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.coverage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.h2.util.New;

/**
 * Tool to instrument java files with profiler calls. The tool can be used for
 * profiling an application and for coverage testing. This class is not used at
 * runtime of the tested application.
 */
public class Coverage {
    private static final String IMPORT = "import " +
            Coverage.class.getPackage().getName() + ".Profile";
    private final ArrayList<String> files = New.arrayList();
    private final ArrayList<String> exclude = New.arrayList();
    private Tokenizer tokenizer;
    private Writer writer;
    private Writer data;
    private String token = "";
    private String add = "";
    private String file;
    private int index;
    private int indent;
    private int line;
    private String last;
    private String word, function;
    private boolean perClass;
    private boolean perFunction = true;

    private void printUsage() {
        System.out
                .println("Usage:\n" +
                        "- copy all your source files to another directory\n" +
                        "  (be careful, they will be modified - don't take originals!)\n" +
                        "- java " + getClass().getName() + " <directory>\n" +
                        "  this will modified the source code and create 'profile.txt'\n" +
                        "- compile the modified source files\n" +
                        "- run your main application\n" +
                        "- after the application exits, a file 'notCovered.txt' is created,\n" +
                        "  which contains the class names, function names and line numbers\n" +
                        "  of code that has not been covered\n\n" +
                        "Options:\n" + "-r     recurse all subdirectories\n" +
                        "-e     exclude files\n" +
                        "-c     coverage on a per-class basis\n" +
                        "-f     coverage on a per-function basis\n" +
                        "<dir>  directory name (. for current directory)");
    }

    /**
     * This method is called when executing this application.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        new Coverage().run(args);
    }

    private void run(String... args) {
        if (args.length == 0 || args[0].equals("-?")) {
            printUsage();
            return;
        }
        Coverage c = new Coverage();
        int recurse = 1;
        for (int i = 0; i < args.length; i++) {
            String s = args[i];
            if (s.equals("-r")) {
                // maximum recurse is 100 subdirectories, that should be enough
                recurse = 100;
            } else if (s.equals("-c")) {
                c.perClass = true;
            } else if (s.equals("-f")) {
                c.perFunction = true;
            } else if (s.equals("-e")) {
                c.addExclude(args[++i]);
            } else {
                c.addDir(s, recurse);
            }
        }
        try {
            c.data = new BufferedWriter(new FileWriter("profile.txt"));
            c.processAll();
            c.data.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addExclude(String fileName) {
        exclude.add(fileName);
    }

    private boolean isExcluded(String s) {
        for (String e : exclude) {
            if (s.startsWith(e)) {
                return true;
            }
        }
        return false;
    }

    private void addDir(String path, int recurse) {
        File f = new File(path);
        if (f.isFile() && path.endsWith(".java")) {
            if (!isExcluded(path)) {
                files.add(path);
            }
        } else if (f.isDirectory() && recurse > 0) {
            for (String name : f.list()) {
                addDir(path + "/" + name, recurse - 1);
            }
        }
    }

    private void processAll() {
        int len = files.size();
        long time = System.nanoTime();
        for (int i = 0; i < len; i++) {
            long t2 = System.nanoTime();
            if (t2 - time > TimeUnit.SECONDS.toNanos(1) || i >= len - 1) {
                System.out.println((i + 1) + " of " + len +
                        " " + (100 * i / len) + "%");
                time = t2;
            }
            String fileName = files.get(i);
            processFile(fileName);
        }
    }

    private void processFile(String name) {
        file = name;
        int i;
        i = file.lastIndexOf('.');
        if (i != -1) {
            file = file.substring(0, i);
        }
        while (true) {
            i = file.indexOf('/');
            if (i < 0) {
                i = file.indexOf('\\');
            }
            if (i < 0) {
                break;
            }
            file = file.substring(0, i) + "." + file.substring(i + 1);
        }
        if (name.endsWith("Coverage.java") ||
                name.endsWith("Tokenizer.java") ||
                name.endsWith("Profile.java")) {
            return;
        }
        File f = new File(name);
        File fileNew = new File(name + ".new");
        try {
            writer = new BufferedWriter(new FileWriter(fileNew));
            Reader r = new BufferedReader(new FileReader(f));
            tokenizer = new Tokenizer(r);
            indent = 0;
            try {
                process();
            } catch (Exception e) {
                r.close();
                writer.close();
                e.printStackTrace();
                printError(e.getMessage());
                throw e;
            }
            r.close();
            writer.close();
            File backup = new File(name + ".bak");
            backup.delete();
            f.renameTo(backup);
            File copy = new File(name);
            fileNew.renameTo(copy);
            if (perClass) {
                nextDebug();
            }
        } catch (Exception e) {
            e.printStackTrace();
            printError(e.getMessage());
        }
    }

    private void read() throws IOException {
        last = token;
        String write = token;
        token = null;
        tokenizer.initToken();
        int i = tokenizer.nextToken();
        if (i != Tokenizer.TYPE_EOF) {
            token = tokenizer.getString();
            if (token == null) {
                token = "" + ((char) i);
            } else if (i == '\'') {
                // mToken="'"+getEscape(mToken)+"'";
                token = tokenizer.getToken();
            } else if (i == '\"') {
                // mToken="\""+getEscape(mToken)+"\"";
                token = tokenizer.getToken();
            } else {
                if (write == null) {
                    write = "";
                } else {
                    write = write + " ";
                }
            }
        }
        if (write == null || (!write.equals("else ") &&
                !write.equals("else") && !write.equals("super ") &&
                !write.equals("super") && !write.equals("this ") &&
                !write.equals("this") && !write.equals("} ") &&
                !write.equals("}"))) {
            if (add != null && !add.equals("")) {
                writeLine();
                write(add);
                if (!perClass) {
                    nextDebug();
                }
            }
        }
        add = "";
        if (write != null) {
            write(write);
        }
    }

    private void readThis(String s) throws IOException {
        if (!token.equals(s)) {
            throw new IOException("Expected: " + s + " got:" + token);
        }
        read();
    }

    private void process() throws IOException {
        boolean imp = false;
        read();
        do {
            while (true) {
                if (token == null || token.equals("{")) {
                    break;
                } else if (token.equals(";")) {
                    if (!imp) {
                        write(";" + IMPORT);
                        imp = true;
                    }
                }
                read();
            }
            processClass();
        } while (token != null);
    }

    private void processInit() throws IOException {
        do {
            if (token.equals("{")) {
                read();
                processInit();
            } else if (token.equals("}")) {
                read();
                return;
            } else {
                read();
            }
        } while (true);
    }

    private void processClass() throws IOException {
        int type = 0;
        while (true) {
            if (token == null) {
                break;
            } else if (token.equals("class")) {
                read();
                type = 1;
            } else if (token.equals("=")) {
                read();
                type = 2;
            } else if (token.equals("static")) {
                word = "static";
                read();
                type = 3;
            } else if (token.equals("(")) {
                word = last + "(";
                read();
                if (!token.equals(")")) {
                    word = word + token;
                }
                type = 3;
            } else if (token.equals(",")) {
                read();
                word = word + "," + token;
            } else if (token.equals(")")) {
                word = word + ")";
                read();
            } else if (token.equals(";")) {
                read();
                type = 0;
            } else if (token.equals("{")) {
                read();
                if (type == 1) {
                    processClass();
                } else if (type == 2) {
                    processInit();
                } else if (type == 3) {
                    writeLine();
                    setLine();
                    processFunction();
                    writeLine();
                }
            } else if (token.equals("}")) {
                read();
                break;
            } else {
                read();
            }
        }
    }

    private void processBracket() throws IOException {
        do {
            if (token.equals("(")) {
                read();
                processBracket();
            } else if (token.equals(")")) {
                read();
                return;
            } else {
                read();
            }
        } while (true);
    }

    private void processFunction() throws IOException {
        function = word;
        writeLine();
        do {
            processStatement();
        } while (!token.equals("}"));
        read();
        writeLine();
    }

    private void processBlockOrStatement() throws IOException {
        if (!token.equals("{")) {
            write("{ //++");
            writeLine();
            setLine();
            processStatement();
            write("} //++");
            writeLine();
        } else {
            read();
            setLine();
            processFunction();
        }
    }

    private void processStatement() throws IOException {
        while (true) {
            if (token.equals("while") || token.equals("for") ||
                    token.equals("synchronized")) {
                read();
                readThis("(");
                processBracket();
                indent++;
                processBlockOrStatement();
                indent--;
                return;
            } else if (token.equals("if")) {
                read();
                readThis("(");
                processBracket();
                indent++;
                processBlockOrStatement();
                indent--;
                if (token.equals("else")) {
                    read();
                    indent++;
                    processBlockOrStatement();
                    indent--;
                }
                return;
            } else if (token.equals("try")) {
                read();
                indent++;
                processBlockOrStatement();
                indent--;
                while (true) {
                    if (token.equals("catch")) {
                        read();
                        readThis("(");
                        processBracket();
                        indent++;
                        processBlockOrStatement();
                        indent--;
                    } else if (token.equals("finally")) {
                        read();
                        indent++;
                        processBlockOrStatement();
                        indent--;
                    } else {
                        break;
                    }
                }
                return;
            } else if (token.equals("{")) {
                if (last.equals(")")) {
                    // process anonymous inner classes (this is a hack)
                    read();
                    processClass();
                    return;
                } else if (last.equals("]")) {
                    // process object array initialization (another hack)
                    while (!token.equals("}")) {
                        read();
                    }
                    read();
                    return;
                }
                indent++;
                processBlockOrStatement();
                indent--;
                return;
            } else if (token.equals("do")) {
                read();
                indent++;
                processBlockOrStatement();
                readThis("while");
                readThis("(");
                processBracket();
                readThis(";");
                setLine();
                indent--;
                return;
            } else if (token.equals("case")) {
                add = "";
                read();
                while (!token.equals(":")) {
                    read();
                }
                read();
                setLine();
            } else if (token.equals("default")) {
                add = "";
                read();
                readThis(":");
                setLine();
            } else if (token.equals("switch")) {
                read();
                readThis("(");
                processBracket();
                indent++;
                processBlockOrStatement();
                indent--;
                return;
            } else if (token.equals("class")) {
                read();
                processClass();
                return;
            } else if (token.equals("(")) {
                read();
                processBracket();
            } else if (token.equals("=")) {
                read();
                if (token.equals("{")) {
                    read();
                    processInit();
                }
            } else if (token.equals(";")) {
                read();
                setLine();
                return;
            } else if (token.equals("}")) {
                return;
            } else {
                read();
            }
        }
    }

    private void setLine() {
        add += "Profile.visit(" + index + ");";
        line = tokenizer.getLine();
    }

    private void nextDebug() throws IOException {
        if (perFunction) {
            int i = function.indexOf('(');
            String func = i < 0 ? function : function.substring(0, i);
            String fileLine = file + "." + func + "(";
            i = file.lastIndexOf('.');
            String className = i < 0 ? file : file.substring(i + 1);
            fileLine += className + ".java:" + line + ")";
            data.write(fileLine + " " + last + "\r\n");
        } else {
            data.write(file + " " + line + "\r\n");
        }
        index++;
    }

    private void writeLine() throws IOException {
        write("\r\n");
        for (int i = 0; i < indent; i++) {
            writer.write(' ');
        }
    }

    private void write(String s) throws IOException {
        writer.write(s);
        // System.out.print(s);
    }

    private void printError(String error) {
        System.out.println("");
        System.out.println("File:" + file);
        System.out.println("ERROR: " + error);
    }
}
