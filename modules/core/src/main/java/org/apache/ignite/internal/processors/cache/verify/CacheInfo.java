package org.apache.ignite.internal.processors.cache.verify;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;

/**
 *
 */
public class CacheInfo implements Serializable {
    private static final long serialVersionUID = 0L;

    private String seqName;

    private long seqVal;

    private String cacheName;

    private int cacheId;

    private String grpName;

    private int grpId;

    private int partitions;

    private int mapped;

    private Map<ClusterNode, int[]> primary;

    private Map<ClusterNode, int[]> backups;

    public List<List<ClusterNode>> assignment;

    public List<List<ClusterNode>> idealAssignment;

    public AffinityTopologyVersion topologyVersion;

    private Map<UUID, Set<Integer>> primaryMap;

    private Map<UUID, Set<Integer>> backupMap;

    private CacheMode mode;

    private int backupsCnt;

    private String affinityClsName;

    private String zone;

    private String cell;

    private String dc;

    public String getSeqName() {
        return seqName;
    }

    public void setSeqName(String seqName) {
        this.seqName = seqName;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public int getCacheId() {
        return cacheId;
    }

    public void setCacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    public String getGrpName() {
        return grpName;
    }

    public void setGrpName(String grpName) {
        this.grpName = grpName;
    }

    public int getGrpId() {
        return grpId;
    }

    public void setGrpId(int grpId) {
        this.grpId = grpId;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getMapped() {
        return mapped;
    }

    public void setMapped(int mapped) {
        this.mapped = mapped;
    }

    public Map<ClusterNode, int[]> getPrimary() {
        return primary;
    }

    public void setPrimary(Map<ClusterNode, int[]> primary) {
        this.primary = primary;
    }

    public Map<ClusterNode, int[]> getBackups() {
        return backups;
    }

    public void setBackups(Map<ClusterNode, int[]> backups) {
        this.backups = backups;
    }

    public List<List<ClusterNode>> getAssignment() {
        return assignment;
    }

    public void setAssignment(List<List<ClusterNode>> assignment) {
        this.assignment = assignment;
    }

    public List<List<ClusterNode>> getIdealAssignment() {
        return idealAssignment;
    }

    public void setIdealAssignment(List<List<ClusterNode>> idealAssignment) {
        this.idealAssignment = idealAssignment;
    }

    public AffinityTopologyVersion getTopologyVersion() {
        return topologyVersion;
    }

    public void setTopologyVersion(AffinityTopologyVersion topologyVersion) {
        this.topologyVersion = topologyVersion;
    }

    public void setPrimaryMap(Map<UUID, Set<Integer>> primaryMap) {
        this.primaryMap = primaryMap;
    }

    public Map<UUID, Set<Integer>> getPrimaryMap() {
        return primaryMap;
    }

    public Map<UUID, Set<Integer>> getBackupMap() {
        return backupMap;
    }

    public void setBackupMap(Map<UUID, Set<Integer>> backupMap) {
        this.backupMap = backupMap;
    }

    public void setSeqVal(long seqVal) {
        this.seqVal = seqVal;
    }

    public long getSeqVal() {
        return seqVal;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getCell() {
        return cell;
    }

    public void setCell(String cell) {
        this.cell = cell;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public CacheMode getMode() {
        return mode;
    }

    public void setMode(CacheMode mode) {
        this.mode = mode;
    }

    public int getBackupsCnt() {
        return backupsCnt;
    }

    public void setBackupsCnt(int backupsCnt) {
        this.backupsCnt = backupsCnt;
    }

    public String getAffinityClsName() {
        return affinityClsName;
    }

    public void setAffinityClsName(String affinityClsName) {
        this.affinityClsName = affinityClsName;
    }

    public void print() {
        if (seqName != null) {
            System.out.println("[seqName=" + getSeqName() + ", curVal=" + seqVal + ']');

            return;
        }

        System.out.println("[cacheName=" + getCacheName() + ", id=" + getCacheId() +
            ", grpName=" + getGrpName() + ", grpId=" + getGrpId() + ", prim=" + getPartitions() +
            ", mapped=" + getMapped() + ", mode=" + getMode() +
            ", backups=" + getBackupsCnt() + ", affCls=" + getAffinityClsName() + ']');

        Map<ClusterNode, int[]> prim = getPrimary();

        if (prim != null) {
            StringBuilder b = new StringBuilder();

            for (Map.Entry<ClusterNode, int[]> entry : prim.entrySet()) {
                b.setLength(0);

                Set<Integer> s = new TreeSet<>();
                for (int e = 0; e < entry.getValue().length; e++)
                    s.add(entry.getValue()[e]);

                b.append("    [node=" + entry.getKey() + ", primary=").append(s);

                final int[] backupParts = getBackups().get(entry.getKey());

                s = new TreeSet<>();
                for (int p : backupParts)
                    s.add(p);

                b.append(", backups=").append(s);

                mapped += entry.getValue().length;

                b.append(System.lineSeparator());

                if (topologyVersion != null && assignment != null) {
                    b.append("    [assignment: size=").append(assignment.size()).append(", topVer=").append(topologyVersion).append(" ");

                    for (int i = 0; i < assignment.size(); i++) {
                        b.append(i).append('=');

                        b.append(F.transform(assignment.get(i), new IgniteClosure<ClusterNode, String>() {
                            @Override public String apply(ClusterNode node) {
                                return U.id8(node.id());
                            }
                        }));

                        if (i != assignment.size() - 1)
                            b.append(", ");
                    }

                    b.append(']').append(System.lineSeparator());

                    b.append("    [idealAssignment: size=").append(idealAssignment.size()).append(", topVer=").append(topologyVersion).append(" ");
                    for (int i = 0; i < idealAssignment.size(); i++) {
                        b.append(i).append('=');

                        b.append(F.transform(idealAssignment.get(i), new IgniteClosure<ClusterNode, String>() {
                            @Override public String apply(ClusterNode node) {
                                return U.id8(node.id());
                            }
                        }));

                        if (i != idealAssignment.size() - 1)
                            b.append(", ");
                    }

                    b.append("    [primary=" + primary);

                    b.append(']').append(System.lineSeparator());
                }

                System.out.println(b.toString());
            }
        }
    }
}