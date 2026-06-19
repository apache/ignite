package org.apache.ignite.internal.classpath;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.junit.Assert.assertEquals;

/** */
public class ClassPathTestUtils {
    /** */
    public static Set<Path> files() throws IOException {
        return Stream.of(
            file(0), file(1025), file(U.MB), file(2 * U.MB), file(2 * U.MB + 42)).collect(Collectors.toSet()
        );
    }

    /** */
    public static Set<String> fileNames(Set<Path> dirs) {
        return dirs.stream().map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
    }

    /** */
    public static String[] fileArg(Set<Path> cpFiles) {
        String[] files = new String[cpFiles.size()];

        int i = 0;

        for (Path p : cpFiles)
            files[i++] = p.toAbsolutePath().toString();

        return files;
    }

    /** */
    private static Path file(long size) throws IOException {
        File f = File.createTempFile(size + "_bytes", ".temp");

        f.deleteOnExit();

        try (OutputStream os = Files.newOutputStream(f.toPath())) {
            long written = 0;

            byte[] batch = new byte[4 * (int)U.KB];

            while (written < size) {
                ThreadLocalRandom.current().nextBytes(batch);

                int toWrite = (int)Math.min(size - written, batch.length);

                os.write(batch, 0, toWrite);

                written += toWrite;
            }
        }

        assert f.length() == size;

        return f.toPath();
    }

    /** */
    public static void checkFilesExists(IgniteEx node, String cpName, Set<Path> cpFiles) throws IOException {
        NodeFileTree ft = node.context().pdsFolderResolver().fileTree();

        Set<Path> nodeCpFiles = Files.list(ft.classPathRoot(cpName).toPath()).collect(Collectors.toSet());

        assertEquals(
            "Files must be deployed on each node",
            fileNames(cpFiles),
            fileNames(nodeCpFiles)
        );

        for (Path cpFile : cpFiles) {
            Path nodeFile = nodeCpFiles.stream()
                .filter(p -> Objects.equals(p.getFileName().toString(), cpFile.getFileName().toString()))
                .findFirst().get();

            assertEquals(Files.size(cpFile), Files.size(nodeFile));
        }
    }
}
