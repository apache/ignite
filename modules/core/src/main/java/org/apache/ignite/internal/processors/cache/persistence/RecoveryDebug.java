package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.TimeZone;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public class RecoveryDebug {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy-HH:mm:ss");

    static {
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Nullable private FileChannel fc;

    public RecoveryDebug(Object constId) {
        try {
            String workDir = U.defaultWorkDirectory();

            File tmpDir = new File(workDir, "tmp");

            if (!tmpDir.exists())
                if (!tmpDir.mkdir())
                    return;

            File f = new File(tmpDir, "recovery-" + constId + "-" + sdf.format(new Date(U.currentTimeMillis())) + ".log");

            f.createNewFile();

            fc = FileChannel.open(Paths.get(f.getPath()), EnumSet.of(CREATE, READ, WRITE));
        }
        catch (IgniteCheckedException | IOException e) {
            fc = null;
        }
    }

    public RecoveryDebug append(TxRecord rec) {
        GridCacheVersion txVer = rec.nearXidVersion();
        return fc == null ? this : appendFile(
            "Tx record " + rec.state() + " [ver=" + txVer.topologyVersion() + " order=" + txVer.order() + " nodeOrder=" +
                txVer.nodeOrder() + "] timestamp " + rec.timestamp()
        );
    }

    public RecoveryDebug append(DataRecord rec, boolean unwrapKeyValue) {
        if (fc == null)
            return this;

        append("Data record\n");

        for (DataEntry dataEntry :  rec.writeEntries())
            append("\t" + dataEntry.op() + " " + dataEntry.nearXidVersion() +
                (unwrapKeyValue ? " " + dataEntry.key() + " " + dataEntry.value() : "") + "\n"
            );

        return this;
    }

    public RecoveryDebug append(Object st) {
        return fc == null ? this : appendFile(st);
    }

    private RecoveryDebug appendFile(Object st) {
        try {
            fc.write(ByteBuffer.wrap(st.toString().getBytes()));
        }
        catch (IOException e) {
            U.error(null, "Fail write to recovery dump file.", e);
        }

        return this;
    }

    public void close() {
        if (fc != null)
            try {
                fc.force(true);

                fc.close();
            }
            catch (IOException e) {
                U.error(null, "Fail close recovery dump file.", e);
            }
    }
}
