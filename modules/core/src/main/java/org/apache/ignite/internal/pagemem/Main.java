package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.util.typedef.internal.CU;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 */
public class Main {

    public static void main(String[] args) {
        long number = 35L, result;

        int PAGE_IDX_SIZE = 32;

        long PAGE_IDX_MASK = ~(-1L << PAGE_IDX_SIZE);

//        result = ~number;
//        System.out.println(CU.cacheId("CACHEGROUP_DICTIONARY"));

        System.out.println("DPLIndex_ru.sbrf.autotransact.model.RequestJournal_DPL_rqUID_DPL_union-module " + CU.cacheId("DPLIndex_ru.sbrf.autotransact.model.RequestJournal_DPL_rqUID_DPL_union-module") + "hash code " + "DPLIndex_ru.sbrf.autotransact.model.RequestJournal_DPL_rqUID_DPL_union-module".hashCode());
        System.out.println("DPL_MAPPING_DATE_TO_REVISION_DPL_union-module " + CU.cacheId("DPL_MAPPING_DATE_TO_REVISION_DPL_union-module"));
        System.out.println("DPartyContactServiceAttributes_DPL_union-module " + CU.cacheId("DPartyContactServiceAttributes_DPL_union-module"));

        System.out.println("CACHEGROUP_UNIQUE_PARTICLE_acquirIOVersionsing-stub " + CU.cacheId("CACHEGROUP_UNIQUE_PARTICLE_acquiring-stub"));

        System.out.println("CACHEGROUP_PARTICLE_processing_com.sbt.acquiring.processing.entities.dictionaries.PublishedDepTerBank " + CU.cacheId("CACHEGROUP_PARTICLE_processing_com.sbt.acquiring.processing.entities.dictionaries.PublishedDepTerBank"));
        System.out.println("CACHEGROUP_PARTICLE_union-module_com.sbt.acquiring.processing.entities.dictionaries.PublishedDepTerBank " + CU.cacheId("CACHEGROUP_PARTICLE_union-module_com.sbt.acquiring.processing.entities.dictionaries.PublishedDepTerBank"));
        System.out.println("CACHEGROUP_DICTIONARY " + CU.cacheId("CACHEGROUP_DICTIONARY"));

        System.out.println("CELL: CELL_AUTO_18 part: 18103 hash: " + hash(18103, "CELL_AUTO_18"));
        System.out.println("CELL: CELL_AUTO_07 part: 18103 hash: " + hash(18103, "CELL_AUTO_07"));

        try {
            File file = new File("C:\\Users\\vladp\\Downloads\\cache_list");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.isEmpty())
                    stringBuffer.append(line)
                        .append(":")
                        .append(CU.cacheId(line.trim()))
                        .append("\n");
            }
            fileReader.close();
            System.out.println("Contents of file:");
            System.out.println(stringBuffer.toString());
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param part Partition.
     * @param obj Object.
     */
    public static long hash(Integer part, Object obj) {
        return xorshift64star(((long)part << 32) | obj.hashCode());
    }

    public static long xorshift64star(long x) {
        x ^= x >>> 12; // a
        x ^= x << 25; // b
        x ^= x >>> 27; // c
        return x * 2685821657736338717L;
    }

}
