/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import cn.hutool.system.SystemUtil;
import io.stuart.Starter;
import io.stuart.consts.SysConst;
import io.stuart.entities.internal.MqttSystemInfo;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

public class SysUtil {

    private static final Runtime runtime = Runtime.getRuntime();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    private static final SystemInfo system = new SystemInfo();

    private static final HardwareAbstractionLayer hardware = system.getHardware();

    private static final OperatingSystem os = system.getOperatingSystem();

    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getUptime() {
        return formatDuring(runtimeMXBean.getUptime());
    }

    public static String getSystime() {
        return format.format(Calendar.getInstance().getTime());
    }

    public static int availableProcessors() {
        return hardware.getProcessor().getLogicalProcessorCount();
    }

    public static String getJavaAndJvmInfo() {
        return getJavaVersion() + SysConst.SLASH + getJvmBit();
    }

    public static String getThreadInfo() {
        return getThreadCount() + SysConst.SLASH + getPeakThreadCount();
    }

    public static String getLoadAverageInfo() {
        double[] loadAverages = hardware.getProcessor().getSystemLoadAverage(3);

        StringBuilder sb = new StringBuilder();

        for (double loadAverage : loadAverages) {
            sb.append(convertLoadAverage(loadAverage));
            sb.append(SysConst.SLASH);
        }

        return sb.toString().substring(0, sb.length() - 1);
    }

    public static String getHeapMemoryInfo() {
        long total = runtime.totalMemory() / SysConst.BYTE_TO_MB;
        long free = runtime.freeMemory() / SysConst.BYTE_TO_MB;
        long max = runtime.maxMemory() / SysConst.BYTE_TO_MB;

        long used = total - free;

        StringBuilder sb = new StringBuilder();
        sb.append(used).append(SysConst.MB);
        sb.append(SysConst.SLASH);
        sb.append(max).append(SysConst.MB);

        return sb.toString();
    }

    public static String getOffHeapMemoryInfo() {
        MemoryUsage usage = memoryMXBean.getNonHeapMemoryUsage();
        long max = usage.getMax() / SysConst.BYTE_TO_MB;
        long used = usage.getUsed() / SysConst.BYTE_TO_MB;

        StringBuilder sb = new StringBuilder();
        sb.append(used).append(SysConst.MB);
        sb.append(SysConst.SLASH);

        if (max <= 0L) {
            sb.append(SysConst.NOT_LIMIT);
        } else {
            sb.append(max).append(SysConst.MB);
        }

        return sb.toString();
    }

    public static String getMaxFileDescriptors() {
        long max = os.getFileSystem().getMaxFileDescriptors();

        if (max > 0) {
            return Long.toString(max);
        } else {
            return SysConst.NOT_APPLICABLE;
        }
    }

    public static MqttSystemInfo getSystemInfo() {
        MqttSystemInfo systemInfo = new MqttSystemInfo();

        systemInfo.setVersion(Starter.class.getPackage().getImplementationVersion());
        systemInfo.setUptime(getUptime());
        systemInfo.setSystime(getSystime());

        return systemInfo;
    }

    private static String formatDuring(long mss) {
        long days = mss / (1000 * 60 * 60 * 24);
        long hours = (mss % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
        long minutes = (mss % (1000 * 60 * 60)) / (1000 * 60);
        long seconds = (mss % (1000 * 60)) / 1000;

        return days + "d, " + hours + "h, " + minutes + "m, " + seconds + "s";
    }

    private static String getJavaVersion() {
        return SystemUtil.getJavaInfo().getVersion();
    }

    private static String getJvmBit() {
        String jvmName = SystemUtil.getJvmInfo().getName();

        if (jvmName.contains(SysConst.BIT_32)) {
            return SysConst.BIT_32;
        } else if (jvmName.contains(SysConst.BIT_64)) {
            return SysConst.BIT_64;
        }

        return jvmName;
    }

    private static int getThreadCount() {
        return threadMXBean.getThreadCount();
    }

    private static int getPeakThreadCount() {
        return threadMXBean.getPeakThreadCount();
    }

    private static String convertLoadAverage(double loadAverage) {
        return loadAverage < 0 ? SysConst.NOT_APPLICABLE : String.format("%.2f", loadAverage);
    }

}
