/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class to get MIME type.
 */
public class VisorMimeTypes {
    /** Bytes to read from file for mimetype detection. */
    private static final int PREVIEW_SIZE = 11;

    /** Common MIME types. */
    private static final Map<String, String> MIME_TYPES = U.newHashMap(810);

    static {
        MIME_TYPES.put("mseed", "application/vnd.fdsn.mseed");
        MIME_TYPES.put("vsf", "application/vnd.vsf");
        MIME_TYPES.put("cmdf", "chemical/x-cmdf");
        MIME_TYPES.put("mxs", "application/vnd.triscape.mxs");
        MIME_TYPES.put("m4v", "video/x-m4v");
        MIME_TYPES.put("oga", "audio/ogg");
        MIME_TYPES.put("ogg", "audio/ogg");
        MIME_TYPES.put("spx", "audio/ogg");
        MIME_TYPES.put("shf", "application/shf+xml");
        MIME_TYPES.put("jisp", "application/vnd.jisp");
        MIME_TYPES.put("sgl", "application/vnd.stardivision.writer-global");
        MIME_TYPES.put("gxt", "application/vnd.geonext");
        MIME_TYPES.put("mp4s", "application/mp4");
        MIME_TYPES.put("smi", "application/smil+xml");
        MIME_TYPES.put("smil", "application/smil+xml");
        MIME_TYPES.put("xop", "application/xop+xml");
        MIME_TYPES.put("spl", "application/x-futuresplash");
        MIME_TYPES.put("spq", "application/scvp-vp-request");
        MIME_TYPES.put("bh2", "application/vnd.fujitsu.oasysprs");
        MIME_TYPES.put("xpm", "image/x-xpixmap");
        MIME_TYPES.put("gdl", "model/vnd.gdl");
        MIME_TYPES.put("dotx", "application/vnd.openxmlformats-officedocument.wordprocessingml.template");
        MIME_TYPES.put("ser", "application/java-serialized-object");
        MIME_TYPES.put("ghf", "application/vnd.groove-help");
        MIME_TYPES.put("mc1", "application/vnd.medcalcdata");
        MIME_TYPES.put("cdy", "application/vnd.cinderella");
        MIME_TYPES.put("nns", "application/vnd.noblenet-sealer");
        MIME_TYPES.put("msty", "application/vnd.muvee.style");
        MIME_TYPES.put("aas", "application/x-authorware-seg");
        MIME_TYPES.put("p", "text/x-pascal");
        MIME_TYPES.put("pas", "text/x-pascal");
        MIME_TYPES.put("rdz", "application/vnd.data-vision.rdz");
        MIME_TYPES.put("js", "text/javascript");
        MIME_TYPES.put("fzs", "application/vnd.fuzzysheet");
        MIME_TYPES.put("csml", "chemical/x-csml");
        MIME_TYPES.put("psf", "application/x-font-linux-psf");
        MIME_TYPES.put("afp", "application/vnd.ibm.modcap");
        MIME_TYPES.put("listafp", "application/vnd.ibm.modcap");
        MIME_TYPES.put("list3820", "application/vnd.ibm.modcap");
        MIME_TYPES.put("qt", "video/quicktime");
        MIME_TYPES.put("mov", "video/quicktime");
        MIME_TYPES.put("fly", "text/vnd.fly");
        MIME_TYPES.put("rlc", "image/vnd.fujixerox.edmics-rlc");
        MIME_TYPES.put("sxi", "application/vnd.sun.xml.impress");
        MIME_TYPES.put("vor", "application/vnd.stardivision.writer");
        MIME_TYPES.put("gac", "application/vnd.groove-account");
        MIME_TYPES.put("sldm", "application/vnd.ms-powerpoint.slide.macroenabled.12");
        MIME_TYPES.put("pgn", "application/x-chess-pgn");
        MIME_TYPES.put("zaz", "application/vnd.zzazz.deck+xml");
        MIME_TYPES.put("ccxml", "application/ccxml+xml");
        MIME_TYPES.put("csh", "application/x-csh");
        MIME_TYPES.put("fvt", "video/vnd.fvt");
        MIME_TYPES.put("grv", "application/vnd.groove-injector");
        MIME_TYPES.put("scurl", "text/vnd.curl.scurl");
        MIME_TYPES.put("oa3", "application/vnd.fujitsu.oasys3");
        MIME_TYPES.put("oa2", "application/vnd.fujitsu.oasys2");
        MIME_TYPES.put("rgb", "image/x-rgb");
        MIME_TYPES.put("pfr", "application/font-tdpfr");
        MIME_TYPES.put("pbd", "application/vnd.powerbuilder6");
        MIME_TYPES.put("psd", "image/vnd.adobe.photoshop");
        MIME_TYPES.put("fh", "image/x-freehand");
        MIME_TYPES.put("fhc", "image/x-freehand");
        MIME_TYPES.put("fh4", "image/x-freehand");
        MIME_TYPES.put("fh5", "image/x-freehand");
        MIME_TYPES.put("fh7", "image/x-freehand");
        MIME_TYPES.put("doc", "application/msword");
        MIME_TYPES.put("dot", "application/msword");
        MIME_TYPES.put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        MIME_TYPES.put("plc", "application/vnd.mobius.plc");
        MIME_TYPES.put("rnc", "application/relax-ng-compact-syntax");
        MIME_TYPES.put("rpss", "application/vnd.nokia.radio-presets");
        MIME_TYPES.put("nlu", "application/vnd.neurolanguage.nlu");
        MIME_TYPES.put("pcl", "application/vnd.hp-pcl");
        MIME_TYPES.put("uu", "text/x-uuencode");
        MIME_TYPES.put("ami", "application/vnd.amiga.ami");
        MIME_TYPES.put("viv", "video/vnd.vivo");
        MIME_TYPES.put("mp4a", "audio/mp4");
        MIME_TYPES.put("vis", "application/vnd.visionary");
        MIME_TYPES.put("asc", "application/pgp-signature");
        MIME_TYPES.put("sig", "application/pgp-signature");
        MIME_TYPES.put("karbon", "application/vnd.kde.karbon");
        MIME_TYPES.put("htke", "application/vnd.kenameaapp");
        MIME_TYPES.put("nnd", "application/vnd.noblenet-directory");
        MIME_TYPES.put("xlsm", "application/vnd.ms-excel.sheet.macroenabled.12");
        MIME_TYPES.put("ez2", "application/vnd.ezpix-album");
        MIME_TYPES.put("otm", "application/vnd.oasis.opendocument.text-master");
        MIME_TYPES.put("f", "text/x-fortran");
        MIME_TYPES.put("for", "text/x-fortran");
        MIME_TYPES.put("f77", "text/x-fortran");
        MIME_TYPES.put("f90", "text/x-fortran");
        MIME_TYPES.put("apr", "application/vnd.lotus-approach");
        MIME_TYPES.put("mid", "audio/midi");
        MIME_TYPES.put("midi", "audio/midi");
        MIME_TYPES.put("kar", "audio/midi");
        MIME_TYPES.put("rmi", "audio/midi");
        MIME_TYPES.put("tmo", "application/vnd.tmobile-livetv");
        MIME_TYPES.put("pya", "audio/vnd.ms-playready.media.pya");
        MIME_TYPES.put("cgm", "image/cgm");
        MIME_TYPES.put("uri", "text/uri-list");
        MIME_TYPES.put("uris", "text/uri-list");
        MIME_TYPES.put("urls", "text/uri-list");
        MIME_TYPES.put("ipk", "application/vnd.shana.informed.package");
        MIME_TYPES.put("clkk", "application/vnd.crick.clicker.keyboard");
        MIME_TYPES.put("mbox", "application/mbox");
        MIME_TYPES.put("unityweb", "application/vnd.unity");
        MIME_TYPES.put("joda", "application/vnd.joost.joda-archive");
        MIME_TYPES.put("kwd", "application/vnd.kde.kword");
        MIME_TYPES.put("kwt", "application/vnd.kde.kword");
        MIME_TYPES.put("mpy", "application/vnd.ibm.minipay");
        MIME_TYPES.put("mpeg", "video/mpeg");
        MIME_TYPES.put("mpg", "video/mpeg");
        MIME_TYPES.put("mpe", "video/mpeg");
        MIME_TYPES.put("m1v", "video/mpeg");
        MIME_TYPES.put("m2v", "video/mpeg");
        MIME_TYPES.put("asf", "video/x-ms-asf");
        MIME_TYPES.put("asx", "video/x-ms-asf");
        MIME_TYPES.put("jad", "text/vnd.sun.j2me.app-descriptor");
        MIME_TYPES.put("rm", "application/vnd.rn-realmedia");
        MIME_TYPES.put("xer", "application/patch-ops-error+xml");
        MIME_TYPES.put("xml", "application/xml");
        MIME_TYPES.put("xsl", "application/xml");
        MIME_TYPES.put("icc", "application/vnd.iccprofile");
        MIME_TYPES.put("icm", "application/vnd.iccprofile");
        MIME_TYPES.put("mmr", "image/vnd.fujixerox.edmics-mmr");
        MIME_TYPES.put("hvd", "application/vnd.yamaha.hv-dic");
        MIME_TYPES.put("mbk", "application/vnd.mobius.mbk");
        MIME_TYPES.put("123", "application/vnd.lotus-1-2-3");
        MIME_TYPES.put("fe_launch", "application/vnd.denovo.fcselayout-link");
        MIME_TYPES.put("setpay", "application/set-payment-initiation");
        MIME_TYPES.put("sdc", "application/vnd.stardivision.calc");
        MIME_TYPES.put("swi", "application/vnd.aristanetworks.swi");
        MIME_TYPES.put("g3", "image/g3fax");
        MIME_TYPES.put("cil", "application/vnd.ms-artgalry");
        MIME_TYPES.put("vcg", "application/vnd.groove-vcard");
        MIME_TYPES.put("chm", "application/vnd.ms-htmlhelp");
        MIME_TYPES.put("grxml", "application/srgs+xml");
        MIME_TYPES.put("ext", "application/vnd.novadigm.ext");
        MIME_TYPES.put("opf", "application/oebps-package+xml");
        MIME_TYPES.put("gtm", "application/vnd.groove-tool-message");
        MIME_TYPES.put("au", "audio/basic");
        MIME_TYPES.put("snd", "audio/basic");
        MIME_TYPES.put("ppsm", "application/vnd.ms-powerpoint.slideshow.macroenabled.12");
        MIME_TYPES.put("wbxml", "application/vnd.wap.wbxml");
        MIME_TYPES.put("oxt", "application/vnd.openofficeorg.extension");
        MIME_TYPES.put("davmount", "application/davmount+xml");
        MIME_TYPES.put("ivu", "application/vnd.immervision-ivu");
        MIME_TYPES.put("wrl", "model/vrml");
        MIME_TYPES.put("vrml", "model/vrml");
        MIME_TYPES.put("p7m", "application/pkcs7-mime");
        MIME_TYPES.put("p7c", "application/pkcs7-mime");
        MIME_TYPES.put("ppt", "application/vnd.ms-powerpoint");
        MIME_TYPES.put("pps", "application/vnd.ms-powerpoint");
        MIME_TYPES.put("pot", "application/vnd.ms-powerpoint");
        MIME_TYPES.put("ivp", "application/vnd.immervision-ivp");
        MIME_TYPES.put("movie", "video/x-sgi-movie");
        MIME_TYPES.put("ecelp4800", "audio/vnd.nuera.ecelp4800");
        MIME_TYPES.put("tpl", "application/vnd.groove-tool-template");
        MIME_TYPES.put("fnc", "application/vnd.frogans.fnc");
        MIME_TYPES.put("wax", "audio/x-ms-wax");
        MIME_TYPES.put("3gp", "video/3gpp");
        MIME_TYPES.put("ppd", "application/vnd.cups-ppd");
        MIME_TYPES.put("mmf", "application/vnd.smaf");
        MIME_TYPES.put("exe", "application/x-msdownload");
        MIME_TYPES.put("dll", "application/x-msdownload");
        MIME_TYPES.put("com", "application/x-msdownload");
        MIME_TYPES.put("bat", "application/x-msdownload");
        MIME_TYPES.put("msi", "application/x-msdownload");
        MIME_TYPES.put("skp", "application/vnd.koan");
        MIME_TYPES.put("skd", "application/vnd.koan");
        MIME_TYPES.put("skt", "application/vnd.koan");
        MIME_TYPES.put("skm", "application/vnd.koan");
        MIME_TYPES.put("ice", "x-conference/x-cooltalk");
        MIME_TYPES.put("emma", "application/emma+xml");
        MIME_TYPES.put("odc", "application/vnd.oasis.opendocument.chart");
        MIME_TYPES.put("atomcat", "application/atomcat+xml");
        MIME_TYPES.put("onetoc", "application/onenote");
        MIME_TYPES.put("onetoc2", "application/onenote");
        MIME_TYPES.put("onetmp", "application/onenote");
        MIME_TYPES.put("onepkg", "application/onenote");
        MIME_TYPES.put("otp", "application/vnd.oasis.opendocument.presentation-template");
        MIME_TYPES.put("irm", "application/vnd.ibm.rights-management");
        MIME_TYPES.put("texinfo", "application/x-texinfo");
        MIME_TYPES.put("texi", "application/x-texinfo");
        MIME_TYPES.put("spp", "application/scvp-vp-response");
        MIME_TYPES.put("xif", "image/vnd.xiff");
        MIME_TYPES.put("sitx", "application/x-stuffitx");
        MIME_TYPES.put("eol", "audio/vnd.digital-winds");
        MIME_TYPES.put("c4g", "application/vnd.clonk.c4group");
        MIME_TYPES.put("c4d", "application/vnd.clonk.c4group");
        MIME_TYPES.put("c4f", "application/vnd.clonk.c4group");
        MIME_TYPES.put("c4p", "application/vnd.clonk.c4group");
        MIME_TYPES.put("c4u", "application/vnd.clonk.c4group");
        MIME_TYPES.put("flo", "application/vnd.micrografx.flo");
        MIME_TYPES.put("svg", "image/svg+xml");
        MIME_TYPES.put("svgz", "image/svg+xml");
        MIME_TYPES.put("xsm", "application/vnd.syncml+xml");
        MIME_TYPES.put("hdf", "application/x-hdf");
        MIME_TYPES.put("cml", "chemical/x-cml");
        MIME_TYPES.put("atx", "application/vnd.antix.game-component");
        MIME_TYPES.put("flv", "video/x-flv");
        MIME_TYPES.put("m3u8", "application/vnd.apple.mpegurl");
        MIME_TYPES.put("wbs", "application/vnd.criticaltools.wbs+xml");
        MIME_TYPES.put("bz2", "application/x-bzip2");
        MIME_TYPES.put("boz", "application/x-bzip2");
        MIME_TYPES.put("plf", "application/vnd.pocketlearn");
        MIME_TYPES.put("lbe", "application/vnd.llamagraphics.life-balance.exchange+xml");
        MIME_TYPES.put("vcd", "application/x-cdlink");
        MIME_TYPES.put("sxg", "application/vnd.sun.xml.writer.global");
        MIME_TYPES.put("fli", "video/x-fli");
        MIME_TYPES.put("wsdl", "application/wsdl+xml");
        MIME_TYPES.put("slt", "application/vnd.epson.salt");
        MIME_TYPES.put("lwp", "application/vnd.lotus-wordpro");
        MIME_TYPES.put("ddd", "application/vnd.fujixerox.ddd");
        MIME_TYPES.put("xfdf", "application/vnd.adobe.xfdf");
        MIME_TYPES.put("portpkg", "application/vnd.macports.portpkg");
        MIME_TYPES.put("pfa", "application/x-font-type1");
        MIME_TYPES.put("pfb", "application/x-font-type1");
        MIME_TYPES.put("pfm", "application/x-font-type1");
        MIME_TYPES.put("afm", "application/x-font-type1");
        MIME_TYPES.put("nc", "application/x-netcdf");
        MIME_TYPES.put("cdf", "application/x-netcdf");
        MIME_TYPES.put("mpm", "application/vnd.blueice.multipass");
        MIME_TYPES.put("sis", "application/vnd.symbian.install");
        MIME_TYPES.put("sisx", "application/vnd.symbian.install");
        MIME_TYPES.put("ustar", "application/x-ustar");
        MIME_TYPES.put("rpst", "application/vnd.nokia.radio-preset");
        MIME_TYPES.put("eml", "message/rfc822");
        MIME_TYPES.put("mime", "message/rfc822");
        MIME_TYPES.put("g2w", "application/vnd.geoplan");
        MIME_TYPES.put("ma", "application/mathematica");
        MIME_TYPES.put("nb", "application/mathematica");
        MIME_TYPES.put("mb", "application/mathematica");
        MIME_TYPES.put("csv", "text/csv");
        MIME_TYPES.put("css", "text/css");
        MIME_TYPES.put("wvx", "video/x-ms-wvx");
        MIME_TYPES.put("jlt", "application/vnd.hp-jlyt");
        MIME_TYPES.put("vcx", "application/vnd.vcx");
        MIME_TYPES.put("html", "text/html");
        MIME_TYPES.put("htm", "text/html");
        MIME_TYPES.put("docm", "application/vnd.ms-word.document.macroenabled.12");
        MIME_TYPES.put("xdssc", "application/dssc+xml");
        MIME_TYPES.put("pbm", "image/x-binary-bitmap");
        MIME_TYPES.put("fdf", "application/vnd.fdf");
        MIME_TYPES.put("ggt", "application/vnd.geogebra.tool");
        MIME_TYPES.put("cii", "application/vnd.anser-web-certificate-issue-initiation");
        MIME_TYPES.put("atomsvc", "application/atomsvc+xml");
        MIME_TYPES.put("stw", "application/vnd.sun.xml.writer.template");
        MIME_TYPES.put("vtu", "model/vnd.vtu");
        MIME_TYPES.put("latex", "application/x-latex");
        MIME_TYPES.put("cat", "application/vnd.ms-pki.seccat");
        MIME_TYPES.put("odf", "application/vnd.oasis.opendocument.formula");
        MIME_TYPES.put("trm", "application/x-msterminal");
        MIME_TYPES.put("pptm", "application/vnd.ms-powerpoint.presentation.macroenabled.12");
        MIME_TYPES.put("stl", "application/vnd.ms-pki.stl");
        MIME_TYPES.put("ltf", "application/vnd.frogans.ltf");
        MIME_TYPES.put("obd", "application/x-msbinder");
        MIME_TYPES.put("sda", "application/vnd.stardivision.draw");
        MIME_TYPES.put("org", "application/vnd.lotus-organizer");
        MIME_TYPES.put("ftc", "application/vnd.fluxtime.clip");
        MIME_TYPES.put("rcprofile", "application/vnd.ipunplugged.rcprofile");
        MIME_TYPES.put("cmx", "image/x-cmx");
        MIME_TYPES.put("cif", "chemical/x-cif");
        MIME_TYPES.put("rp9", "application/vnd.cloanto.rp9");
        MIME_TYPES.put("pvb", "application/vnd.3gpp.pic-bw-var");
        MIME_TYPES.put("aw", "application/applixware");
        MIME_TYPES.put("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation");
        MIME_TYPES.put("rld", "application/resource-lists-diff+xml");
        MIME_TYPES.put("xar", "application/vnd.xara");
        MIME_TYPES.put("ecelp7470", "audio/vnd.nuera.ecelp7470");
        MIME_TYPES.put("xls", "application/vnd.ms-excel");
        MIME_TYPES.put("xlm", "application/vnd.ms-excel");
        MIME_TYPES.put("xla", "application/vnd.ms-excel");
        MIME_TYPES.put("xlc", "application/vnd.ms-excel");
        MIME_TYPES.put("xlt", "application/vnd.ms-excel");
        MIME_TYPES.put("xlw", "application/vnd.ms-excel");
        MIME_TYPES.put("txf", "application/vnd.mobius.txf");
        MIME_TYPES.put("prc", "application/x-mobipocket-ebook");
        MIME_TYPES.put("mobi", "application/x-mobipocket-ebook");
        MIME_TYPES.put("swf", "application/x-shockwave-flash");
        MIME_TYPES.put("sfs", "application/vnd.spotfire.sfs");
        MIME_TYPES.put("dp", "application/vnd.osgi.dp");
        MIME_TYPES.put("potx", "application/vnd.openxmlformats-officedocument.presentationml.template");
        MIME_TYPES.put("efif", "application/vnd.picsel");
        MIME_TYPES.put("pdf", "application/pdf");
        MIME_TYPES.put("dsc", "text/prs.lines.tag");
        MIME_TYPES.put("mathml", "application/mathml+xml");
        MIME_TYPES.put("bed", "application/vnd.realvnc.bed");
        MIME_TYPES.put("bcpio", "application/x-bcpio");
        MIME_TYPES.put("shar", "application/x-shar");
        MIME_TYPES.put("xdm", "application/vnd.syncml.dm+xml");
        MIME_TYPES.put("teacher", "application/vnd.smart.teacher");
        MIME_TYPES.put("sldx", "application/vnd.openxmlformats-officedocument.presentationml.slide");
        MIME_TYPES.put("stf", "application/vnd.wt.stf");
        MIME_TYPES.put("odb", "application/vnd.oasis.opendocument.database");
        MIME_TYPES.put("bdf", "application/x-font-bdf");
        MIME_TYPES.put("tcap", "application/vnd.3gpp2.tcap");
        MIME_TYPES.put("fm", "application/vnd.framemaker");
        MIME_TYPES.put("frame", "application/vnd.framemaker");
        MIME_TYPES.put("maker", "application/vnd.framemaker");
        MIME_TYPES.put("book", "application/vnd.framemaker");
        MIME_TYPES.put("src", "application/x-wais-source");
        MIME_TYPES.put("rep", "application/vnd.businessobjects");
        MIME_TYPES.put("dna", "application/vnd.dna");
        MIME_TYPES.put("jpgv", "video/jpeg");
        MIME_TYPES.put("ez", "application/andrew-inset");
        MIME_TYPES.put("vxml", "application/voicexml+xml");
        MIME_TYPES.put("x3d", "application/vnd.hzn-3d-crossword");
        MIME_TYPES.put("dxp", "application/vnd.spotfire.dxp");
        MIME_TYPES.put("dvi", "application/x-dvi");
        MIME_TYPES.put("f4v", "video/x-f4v");
        MIME_TYPES.put("fg5", "application/vnd.fujitsu.oasysgp");
        MIME_TYPES.put("clp", "application/x-msclip");
        MIME_TYPES.put("acu", "application/vnd.acucobol");
        MIME_TYPES.put("ksp", "application/vnd.kde.kspread");
        MIME_TYPES.put("rms", "application/vnd.jcp.javame.midlet-rms");
        MIME_TYPES.put("lvp", "audio/vnd.lucent.voice");
        MIME_TYPES.put("jpm", "video/jpm");
        MIME_TYPES.put("jpgm", "video/jpm");
        MIME_TYPES.put("pcx", "image/x-pcx");
        MIME_TYPES.put("sxm", "application/vnd.sun.xml.math");
        MIME_TYPES.put("g3w", "application/vnd.geospace");
        MIME_TYPES.put("ngdat", "application/vnd.nokia.n-gage.data");
        MIME_TYPES.put("clkp", "application/vnd.crick.clicker.palette");
        MIME_TYPES.put("osfpvg", "application/vnd.yamaha.openscoreformat.osfpvg+xml");
        MIME_TYPES.put("aep", "application/vnd.audiograph");
        MIME_TYPES.put("wqd", "application/vnd.wqd");
        MIME_TYPES.put("mdi", "image/vnd.ms-modi");
        MIME_TYPES.put("ecelp9600", "audio/vnd.nuera.ecelp9600");
        MIME_TYPES.put("fig", "application/x-xfig");
        MIME_TYPES.put("p7r", "application/x-pkcs7-certreqresp");
        MIME_TYPES.put("jar", "application/java-archive");
        MIME_TYPES.put("ief", "image/ief");
        MIME_TYPES.put("hvs", "application/vnd.yamaha.hv-script");
        MIME_TYPES.put("cdx", "chemical/x-cdx");
        MIME_TYPES.put("abw", "application/x-abiword");
        MIME_TYPES.put("deb", "application/x-debian-package");
        MIME_TYPES.put("udeb", "application/x-debian-package");
        MIME_TYPES.put("djvu", "image/vnd.djvu");
        MIME_TYPES.put("djv", "image/vnd.djvu");
        MIME_TYPES.put("wml", "text/vnd.wap.wml");
        MIME_TYPES.put("xhtml", "application/xhtml+xml");
        MIME_TYPES.put("xht", "application/xhtml+xml");
        MIME_TYPES.put("tpt", "application/vnd.trid.tpt");
        MIME_TYPES.put("wmz", "application/x-ms-wmz");
        MIME_TYPES.put("rtx", "text/richtext");
        MIME_TYPES.put("wspolicy", "application/wspolicy+xml");
        MIME_TYPES.put("odg", "application/vnd.oasis.opendocument.graphics");
        MIME_TYPES.put("res", "application/x-dtbresource+xml");
        MIME_TYPES.put("xbm", "image/x-xbitmap");
        MIME_TYPES.put("zir", "application/vnd.zul");
        MIME_TYPES.put("zirz", "application/vnd.zul");
        MIME_TYPES.put("cdkey", "application/vnd.mediastation.cdkey");
        MIME_TYPES.put("wmd", "application/x-ms-wmd");
        MIME_TYPES.put("ogv", "video/ogg");
        MIME_TYPES.put("scq", "application/scvp-cv-request");
        MIME_TYPES.put("sfd-hdstx", "application/vnd.hydrostatix.sof-data");
        MIME_TYPES.put("igx", "application/vnd.micrografx.igx");
        MIME_TYPES.put("xps", "application/vnd.ms-xpsdocument");
        MIME_TYPES.put("xdw", "application/vnd.fujixerox.docuworks");
        MIME_TYPES.put("kfo", "application/vnd.kde.kformula");
        MIME_TYPES.put("chrt", "application/vnd.kde.kchart");
        MIME_TYPES.put("sdp", "application/sdp");
        MIME_TYPES.put("oth", "application/vnd.oasis.opendocument.text-web");
        MIME_TYPES.put("3g2", "video/3gpp2");
        MIME_TYPES.put("utz", "application/vnd.uiq.theme");
        MIME_TYPES.put("mus", "application/vnd.musician");
        MIME_TYPES.put("wpd", "application/vnd.wordperfect");
        MIME_TYPES.put("oas", "application/vnd.fujitsu.oasys");
        MIME_TYPES.put("pic", "image/x-pict");
        MIME_TYPES.put("pct", "image/x-pict");
        MIME_TYPES.put("wmx", "video/x-ms-wmx");
        MIME_TYPES.put("p10", "application/pkcs10");
        MIME_TYPES.put("wmv", "video/x-ms-wmv");
        MIME_TYPES.put("xfdl", "application/vnd.xfdl");
        MIME_TYPES.put("pgp", "application/pgp-encrypted");
        MIME_TYPES.put("rs", "application/rls-services+xml");
        MIME_TYPES.put("cpt", "application/mac-compactpro");
        MIME_TYPES.put("gmx", "application/vnd.gmx");
        MIME_TYPES.put("potm", "application/vnd.ms-powerpoint.template.macroenabled.12");
        MIME_TYPES.put("iif", "application/vnd.shana.informed.interchange");
        MIME_TYPES.put("ace", "application/x-ace-compressed");
        MIME_TYPES.put("pcurl", "application/vnd.curl.pcurl");
        MIME_TYPES.put("ods", "application/vnd.oasis.opendocument.spreadsheet");
        MIME_TYPES.put("vsd", "application/vnd.visio");
        MIME_TYPES.put("vst", "application/vnd.visio");
        MIME_TYPES.put("vss", "application/vnd.visio");
        MIME_TYPES.put("vsw", "application/vnd.visio");
        MIME_TYPES.put("ifm", "application/vnd.shana.informed.formdata");
        MIME_TYPES.put("fbs", "image/vnd.fastbidsheet");
        MIME_TYPES.put("qxd", "application/vnd.quark.quarkxpress");
        MIME_TYPES.put("qxt", "application/vnd.quark.quarkxpress");
        MIME_TYPES.put("qwd", "application/vnd.quark.quarkxpress");
        MIME_TYPES.put("qwt", "application/vnd.quark.quarkxpress");
        MIME_TYPES.put("qxl", "application/vnd.quark.quarkxpress");
        MIME_TYPES.put("qxb", "application/vnd.quark.quarkxpress");
        MIME_TYPES.put("epub", "application/epub+zip");
        MIME_TYPES.put("crl", "application/pkix-crl");
        MIME_TYPES.put("nnw", "application/vnd.noblenet-web");
        MIME_TYPES.put("nbp", "application/vnd.wolfram.player");
        MIME_TYPES.put("plb", "application/vnd.3gpp.pic-bw-large");
        MIME_TYPES.put("itp", "application/vnd.shana.informed.formtemplate");
        MIME_TYPES.put("snf", "application/x-font-snf");
        MIME_TYPES.put("str", "application/vnd.pg.format");
        MIME_TYPES.put("wtb", "application/vnd.webturbo");
        MIME_TYPES.put("osf", "application/vnd.yamaha.openscoreformat");
        MIME_TYPES.put("xltx", "application/vnd.openxmlformats-officedocument.spreadsheetml.template");
        MIME_TYPES.put("pre", "application/vnd.lotus-freelance");
        MIME_TYPES.put("clkt", "application/vnd.crick.clicker.template");
        MIME_TYPES.put("hbci", "application/vnd.hbci");
        MIME_TYPES.put("dwf", "model/vnd.dwf");
        MIME_TYPES.put("igs", "model/iges");
        MIME_TYPES.put("iges", "model/iges");
        MIME_TYPES.put("ktz", "application/vnd.kahootz");
        MIME_TYPES.put("ktr", "application/vnd.kahootz");
        MIME_TYPES.put("n-gage", "application/vnd.nokia.n-gage.symbian.install");
        MIME_TYPES.put("otg", "application/vnd.oasis.opendocument.graphics-template");
        MIME_TYPES.put("rsd", "application/rsd+xml");
        MIME_TYPES.put("hlp", "application/winhlp");
        MIME_TYPES.put("azw", "application/vnd.amazon.ebook");
        MIME_TYPES.put("msf", "application/vnd.epson.msf");
        MIME_TYPES.put("mp4", "video/mp4");
        MIME_TYPES.put("mp4v", "video/mp4");
        MIME_TYPES.put("mpg4", "video/mp4");
        MIME_TYPES.put("cod", "application/vnd.rim.cod");
        MIME_TYPES.put("st", "application/vnd.sailingtracker.track");
        MIME_TYPES.put("odi", "application/vnd.oasis.opendocument.image");
        MIME_TYPES.put("tra", "application/vnd.trueapp");
        MIME_TYPES.put("wm", "video/x-ms-wm");
        MIME_TYPES.put("bin", "application/octet-stream");
        MIME_TYPES.put("dms", "application/octet-stream");
        MIME_TYPES.put("lha", "application/octet-stream");
        MIME_TYPES.put("lrf", "application/octet-stream");
        MIME_TYPES.put("lzh", "application/octet-stream");
        MIME_TYPES.put("so", "application/octet-stream");
        MIME_TYPES.put("iso", "application/octet-stream");
        MIME_TYPES.put("dmg", "application/octet-stream");
        MIME_TYPES.put("dist", "application/octet-stream");
        MIME_TYPES.put("distz", "application/octet-stream");
        MIME_TYPES.put("pkg", "application/octet-stream");
        MIME_TYPES.put("bpk", "application/octet-stream");
        MIME_TYPES.put("dump", "application/octet-stream");
        MIME_TYPES.put("elc", "application/octet-stream");
        MIME_TYPES.put("deploy", "application/octet-stream");
        MIME_TYPES.put("tsv", "text/tab-separated-values");
        MIME_TYPES.put("esf", "application/vnd.epson.esf");
        MIME_TYPES.put("p7s", "application/pkcs7-signature");
        MIME_TYPES.put("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        MIME_TYPES.put("geo", "application/vnd.dynageo");
        MIME_TYPES.put("mmd", "application/vnd.chipnuts.karaoke-mmd");
        MIME_TYPES.put("mxml", "application/xv+xml");
        MIME_TYPES.put("xhvml", "application/xv+xml");
        MIME_TYPES.put("xvml", "application/xv+xml");
        MIME_TYPES.put("xvm", "application/xv+xml");
        MIME_TYPES.put("nsf", "application/vnd.lotus-notes");
        MIME_TYPES.put("crd", "application/x-mscardfile");
        MIME_TYPES.put("p12", "application/x-pkcs12");
        MIME_TYPES.put("pfx", "application/x-pkcs12");
        MIME_TYPES.put("c", "text/x-c");
        MIME_TYPES.put("cc", "text/x-c");
        MIME_TYPES.put("cxx", "text/x-c");
        MIME_TYPES.put("cpp", "text/x-c");
        MIME_TYPES.put("h", "text/x-c");
        MIME_TYPES.put("hh", "text/x-c");
        MIME_TYPES.put("dic", "text/x-c");
        MIME_TYPES.put("cla", "application/vnd.claymore");
        MIME_TYPES.put("avi", "video/x-msvideo");
        MIME_TYPES.put("edx", "application/vnd.novadigm.edx");
        MIME_TYPES.put("gph", "application/vnd.flographit");
        MIME_TYPES.put("rq", "application/sparql-query");
        MIME_TYPES.put("xdp", "application/vnd.adobe.xdp+xml");
        MIME_TYPES.put("umj", "application/vnd.umajin");
        MIME_TYPES.put("pclxl", "application/vnd.hp-pclxl");
        MIME_TYPES.put("edm", "application/vnd.novadigm.edm");
        MIME_TYPES.put("gif", "image/gif");
        MIME_TYPES.put("jpeg", "image/jpeg");
        MIME_TYPES.put("jpg", "image/jpeg");
        MIME_TYPES.put("jpe", "image/jpeg");
        MIME_TYPES.put("adp", "audio/adpcm");
        MIME_TYPES.put("txt", "text/plain");
        MIME_TYPES.put("text", "text/plain");
        MIME_TYPES.put("conf", "text/plain");
        MIME_TYPES.put("def", "text/plain");
        MIME_TYPES.put("list", "text/plain");
        MIME_TYPES.put("log", "text/plain");
        MIME_TYPES.put("in", "text/plain");
        MIME_TYPES.put("gsf", "application/x-font-ghostscript");
        MIME_TYPES.put("aab", "application/x-authorware-bin");
        MIME_TYPES.put("x32", "application/x-authorware-bin");
        MIME_TYPES.put("u32", "application/x-authorware-bin");
        MIME_TYPES.put("vox", "application/x-authorware-bin");
        MIME_TYPES.put("ott", "application/vnd.oasis.opendocument.text-template");
        MIME_TYPES.put("kmz", "application/vnd.google-earth.kmz");
        MIME_TYPES.put("etx", "text/x-setext");
        MIME_TYPES.put("sv4crc", "application/x-sv4crc");
        MIME_TYPES.put("xlam", "application/vnd.ms-excel.addin.macroenabled.12");
        MIME_TYPES.put("pcf", "application/x-font-pcf");
        MIME_TYPES.put("torrent", "application/x-bittorrent");
        MIME_TYPES.put("twd", "application/vnd.simtech-mindmapper");
        MIME_TYPES.put("twds", "application/vnd.simtech-mindmapper");
        MIME_TYPES.put("qam", "application/vnd.epson.quickanime");
        MIME_TYPES.put("bdm", "application/vnd.syncml.dm+wbxml");
        MIME_TYPES.put("3dml", "text/vnd.in3d.3dml");
        MIME_TYPES.put("saf", "application/vnd.yamaha.smaf-audio");
        MIME_TYPES.put("ncx", "application/x-dtbncx+xml");
        MIME_TYPES.put("ppsx", "application/vnd.openxmlformats-officedocument.presentationml.slideshow");
        MIME_TYPES.put("sc", "application/vnd.ibm.secure-container");
        MIME_TYPES.put("gnumeric", "application/x-gnumeric");
        MIME_TYPES.put("paw", "application/vnd.pawaafile");
        MIME_TYPES.put("bmp", "image/bmp");
        MIME_TYPES.put("bmi", "application/vnd.bmi");
        MIME_TYPES.put("sxc", "application/vnd.sun.xml.calc");
        MIME_TYPES.put("gim", "application/vnd.groove-identity-message");
        MIME_TYPES.put("qps", "application/vnd.publishare-delta-tree");
        MIME_TYPES.put("scd", "application/x-msschedule");
        MIME_TYPES.put("mgz", "application/vnd.proteus.magazine");
        MIME_TYPES.put("vcf", "text/x-vcard");
        MIME_TYPES.put("flw", "application/vnd.kde.kivio");
        MIME_TYPES.put("xslt", "application/xslt+xml");
        MIME_TYPES.put("stc", "application/vnd.sun.xml.calc.template");
        MIME_TYPES.put("dir", "application/x-director");
        MIME_TYPES.put("dcr", "application/x-director");
        MIME_TYPES.put("dxr", "application/x-director");
        MIME_TYPES.put("cst", "application/x-director");
        MIME_TYPES.put("cct", "application/x-director");
        MIME_TYPES.put("cxt", "application/x-director");
        MIME_TYPES.put("w3d", "application/x-director");
        MIME_TYPES.put("fgd", "application/x-director");
        MIME_TYPES.put("swa", "application/x-director");
        MIME_TYPES.put("rif", "application/reginfo+xml");
        MIME_TYPES.put("atc", "application/vnd.acucorp");
        MIME_TYPES.put("acutc", "application/vnd.acucorp");
        MIME_TYPES.put("fti", "application/vnd.anser-web-funds-transfer-initiation");
        MIME_TYPES.put("dis", "application/vnd.mobius.dis");
        MIME_TYPES.put("zmm", "application/vnd.handheld-entertainment+xml");
        MIME_TYPES.put("mag", "application/vnd.ecowin.chart");
        MIME_TYPES.put("mlp", "application/vnd.dolby.mlp");
        MIME_TYPES.put("psb", "application/vnd.3gpp.pic-bw-small");
        MIME_TYPES.put("mdb", "application/x-msaccess");
        MIME_TYPES.put("aso", "application/vnd.accpac.simply.aso");
        MIME_TYPES.put("pwn", "application/vnd.3m.post-it-notes");
        MIME_TYPES.put("ots", "application/vnd.oasis.opendocument.spreadsheet-template");
        MIME_TYPES.put("mj2", "video/mj2");
        MIME_TYPES.put("mjp2", "video/mj2");
        MIME_TYPES.put("link66", "application/vnd.route66.link66+xml");
        MIME_TYPES.put("les", "application/vnd.hhe.lesson-player");
        MIME_TYPES.put("sti", "application/vnd.sun.xml.impress.template");
        MIME_TYPES.put("cdbcmsg", "application/vnd.contact.cmsg");
        MIME_TYPES.put("cdxml", "application/vnd.chemdraw+xml");
        MIME_TYPES.put("aac", "audio/x-aac");
        MIME_TYPES.put("ipfix", "application/ipfix");
        MIME_TYPES.put("mxu", "video/vnd.mpegurl");
        MIME_TYPES.put("m4u", "video/vnd.mpegurl");
        MIME_TYPES.put("cer", "application/pkix-cert");
        MIME_TYPES.put("mny", "application/x-msmoney");
        MIME_TYPES.put("tex", "application/x-tex");
        MIME_TYPES.put("ics", "text/calendar");
        MIME_TYPES.put("ifb", "text/calendar");
        MIME_TYPES.put("ei6", "application/vnd.pg.osasli");
        MIME_TYPES.put("spf", "application/vnd.yamaha.smaf-phrase");
        MIME_TYPES.put("lostxml", "application/lost+xml");
        MIME_TYPES.put("apk", "application/vnd.android.package-archive");
        MIME_TYPES.put("pub", "application/x-mspublisher");
        MIME_TYPES.put("gtw", "model/vnd.gtw");
        MIME_TYPES.put("ufd", "application/vnd.ufdl");
        MIME_TYPES.put("ufdl", "application/vnd.ufdl");
        MIME_TYPES.put("jnlp", "application/x-java-jnlp-file");
        MIME_TYPES.put("air", "application/vnd.adobe.air-application-installer-package+zip");
        MIME_TYPES.put("dcurl", "text/vnd.curl.dcurl");
        MIME_TYPES.put("mxl", "application/vnd.recordare.musicxml");
        MIME_TYPES.put("cpio", "application/x-cpio");
        MIME_TYPES.put("mpp", "application/vnd.ms-project");
        MIME_TYPES.put("mpt", "application/vnd.ms-project");
        MIME_TYPES.put("pdb", "application/vnd.palm");
        MIME_TYPES.put("pqa", "application/vnd.palm");
        MIME_TYPES.put("oprc", "application/vnd.palm");
        MIME_TYPES.put("mrc", "application/marc");
        MIME_TYPES.put("hqx", "application/mac-binhex40");
        MIME_TYPES.put("json", "application/json");
        MIME_TYPES.put("mvb", "application/x-msmediaview");
        MIME_TYPES.put("m13", "application/x-msmediaview");
        MIME_TYPES.put("m14", "application/x-msmediaview");
        MIME_TYPES.put("mpga", "audio/mpeg");
        MIME_TYPES.put("mp2", "audio/mpeg");
        MIME_TYPES.put("mp2a", "audio/mpeg");
        MIME_TYPES.put("mp3", "audio/mpeg");
        MIME_TYPES.put("m2a", "audio/mpeg");
        MIME_TYPES.put("m3a", "audio/mpeg");
        MIME_TYPES.put("cmc", "application/vnd.cosmocaller");
        MIME_TYPES.put("wmf", "application/x-msmetafile");
        MIME_TYPES.put("odft", "application/vnd.oasis.opendocument.formula-template");
        MIME_TYPES.put("dtb", "application/x-dtbook+xml");
        MIME_TYPES.put("xbap", "application/x-ms-xbap");
        MIME_TYPES.put("csp", "application/vnd.commonspace");
        MIME_TYPES.put("stk", "application/hyperstudio");
        MIME_TYPES.put("mpn", "application/vnd.mophun.application");
        MIME_TYPES.put("gqf", "application/vnd.grafeq");
        MIME_TYPES.put("gqs", "application/vnd.grafeq");
        MIME_TYPES.put("rl", "application/resource-lists+xml");
        MIME_TYPES.put("gtar", "application/x-gtar");
        MIME_TYPES.put("pls", "application/pls+xml");
        MIME_TYPES.put("tcl", "application/x-tcl");
        MIME_TYPES.put("der", "application/x-x509-ca-cert");
        MIME_TYPES.put("crt", "application/x-x509-ca-cert");
        MIME_TYPES.put("xo", "application/vnd.olpc-sugar");
        MIME_TYPES.put("xltm", "application/vnd.ms-excel.template.macroenabled.12");
        MIME_TYPES.put("cu", "application/cu-seeme");
        MIME_TYPES.put("kml", "application/vnd.google-earth.kml+xml");
        MIME_TYPES.put("sh", "application/x-sh");
        MIME_TYPES.put("xpw", "application/vnd.intercon.formnet");
        MIME_TYPES.put("xpx", "application/vnd.intercon.formnet");
        MIME_TYPES.put("wg", "application/vnd.pmi.widget");
        MIME_TYPES.put("seed", "application/vnd.fdsn.seed");
        MIME_TYPES.put("dataless", "application/vnd.fdsn.seed");
        MIME_TYPES.put("sdd", "application/vnd.stardivision.impress");
        MIME_TYPES.put("ttf", "application/x-font-ttf");
        MIME_TYPES.put("ttc", "application/x-font-ttf");
        MIME_TYPES.put("npx", "image/vnd.net-fpx");
        MIME_TYPES.put("aif", "audio/x-aiff");
        MIME_TYPES.put("aiff", "audio/x-aiff");
        MIME_TYPES.put("aifc", "audio/x-aiff");
        MIME_TYPES.put("xlsb", "application/vnd.ms-excel.sheet.binary.macroenabled.12");
        MIME_TYPES.put("wbmp", "image/vnd.wap.wbmp");
        MIME_TYPES.put("rtf", "application/rtf");
        MIME_TYPES.put("sus", "application/vnd.sus-calendar");
        MIME_TYPES.put("susp", "application/vnd.sus-calendar");
        MIME_TYPES.put("prf", "application/pics-rules");
        MIME_TYPES.put("tar", "application/x-tar");
        MIME_TYPES.put("pml", "application/vnd.ctc-posml");
        MIME_TYPES.put("ims", "application/vnd.ms-ims");
        MIME_TYPES.put("imp", "application/vnd.accpac.simply.imp");
        MIME_TYPES.put("xul", "application/vnd.mozilla.xul+xml");
        MIME_TYPES.put("acc", "application/vnd.americandynamics.acc");
        MIME_TYPES.put("mfm", "application/vnd.mfmp");
        MIME_TYPES.put("dotm", "application/vnd.ms-word.template.macroenabled.12");
        MIME_TYPES.put("ptid", "application/vnd.pvi.ptid1");
        MIME_TYPES.put("pyv", "video/vnd.ms-playready.media.pyv");
        MIME_TYPES.put("ssf", "application/vnd.epson.ssf");
        MIME_TYPES.put("sxd", "application/vnd.sun.xml.draw");
        MIME_TYPES.put("xap", "application/x-silverlight-app");
        MIME_TYPES.put("fst", "image/vnd.fst");
        MIME_TYPES.put("rdf", "application/rdf+xml");
        MIME_TYPES.put("gv", "text/vnd.graphviz");
        MIME_TYPES.put("lrm", "application/vnd.ms-lrm");
        MIME_TYPES.put("box", "application/vnd.previewsystems.box");
        MIME_TYPES.put("mseq", "application/vnd.mseq");
        MIME_TYPES.put("xwd", "image/x-xwindowdump");
        MIME_TYPES.put("mscml", "application/mediaservercontrol+xml");
        MIME_TYPES.put("cmp", "application/vnd.yellowriver-custom-menu");
        MIME_TYPES.put("wad", "application/x-doom");
        MIME_TYPES.put("svd", "application/vnd.svd");
        MIME_TYPES.put("pki", "application/pkixcmp");
        MIME_TYPES.put("ai", "application/postscript");
        MIME_TYPES.put("eps", "application/postscript");
        MIME_TYPES.put("ps", "application/postscript");
        MIME_TYPES.put("msl", "application/vnd.mobius.msl");
        MIME_TYPES.put("sv4cpio", "application/x-sv4cpio");
        MIME_TYPES.put("java", "text/x-java-source");
        MIME_TYPES.put("mpc", "application/vnd.mophun.certificate");
        MIME_TYPES.put("daf", "application/vnd.mobius.daf");
        MIME_TYPES.put("qfx", "application/vnd.intu.qfx");
        MIME_TYPES.put("mxf", "application/mxf");
        MIME_TYPES.put("mif", "application/vnd.mif");
        MIME_TYPES.put("txd", "application/vnd.genomatix.tuxedo");
        MIME_TYPES.put("pkipath", "application/pkix-pkipath");
        MIME_TYPES.put("sse", "application/vnd.kodak-descriptor");
        MIME_TYPES.put("kon", "application/vnd.kde.kontour");
        MIME_TYPES.put("dfac", "application/vnd.dreamfactory");
        MIME_TYPES.put("gram", "application/srgs");
        MIME_TYPES.put("hps", "application/vnd.hp-hps");
        MIME_TYPES.put("cab", "application/vnd.ms-cab-compressed");
        MIME_TYPES.put("m3u", "audio/x-mpegurl");
        MIME_TYPES.put("odp", "application/vnd.oasis.opendocument.presentation");
        MIME_TYPES.put("ggb", "application/vnd.geogebra.file");
        MIME_TYPES.put("xyz", "chemical/x-xyz");
        MIME_TYPES.put("clkw", "application/vnd.crick.clicker.wordbank");
        MIME_TYPES.put("mqy", "application/vnd.mobius.mqy");
        MIME_TYPES.put("ico", "image/x-icon");
        MIME_TYPES.put("png", "image/png");
        MIME_TYPES.put("wmlc", "application/vnd.wap.wmlc");
        MIME_TYPES.put("kne", "application/vnd.kinar");
        MIME_TYPES.put("knp", "application/vnd.kinar");
        MIME_TYPES.put("kpr", "application/vnd.kde.kpresenter");
        MIME_TYPES.put("kpt", "application/vnd.kde.kpresenter");
        MIME_TYPES.put("sbml", "application/sbml+xml");
        MIME_TYPES.put("fpx", "image/vnd.fpx");
        MIME_TYPES.put("bz", "application/x-bzip");
        MIME_TYPES.put("flx", "text/vnd.fmi.flexstor");
        MIME_TYPES.put("application", "application/x-ms-application");
        MIME_TYPES.put("wmlsc", "application/vnd.wap.wmlscriptc");
        MIME_TYPES.put("lbd", "application/vnd.llamagraphics.life-balance.desktop");
        MIME_TYPES.put("sxw", "application/vnd.sun.xml.writer");
        MIME_TYPES.put("jam", "application/vnd.jam");
        MIME_TYPES.put("musicxml", "application/vnd.recordare.musicxml+xml");
        MIME_TYPES.put("see", "application/vnd.seemail");
        MIME_TYPES.put("irp", "application/vnd.irepository.package+xml");
        MIME_TYPES.put("tiff", "image/tiff");
        MIME_TYPES.put("tif", "image/tiff");
        MIME_TYPES.put("aam", "application/x-authorware-map");
        MIME_TYPES.put("chat", "application/x-chat");
        MIME_TYPES.put("mpkg", "application/vnd.apple.installer+xml");
        MIME_TYPES.put("otc", "application/vnd.oasis.opendocument.chart-template");
        MIME_TYPES.put("msh", "model/mesh");
        MIME_TYPES.put("mesh", "model/mesh");
        MIME_TYPES.put("silo", "model/mesh");
        MIME_TYPES.put("t", "text/troff");
        MIME_TYPES.put("tr", "text/troff");
        MIME_TYPES.put("roff", "text/troff");
        MIME_TYPES.put("man", "text/troff");
        MIME_TYPES.put("me", "text/troff");
        MIME_TYPES.put("ms", "text/troff");
        MIME_TYPES.put("dpg", "application/vnd.dpgraph");
        MIME_TYPES.put("wri", "application/x-mswrite");
        MIME_TYPES.put("dts", "audio/vnd.dts");
        MIME_TYPES.put("xpi", "application/x-xpinstall");
        MIME_TYPES.put("ram", "audio/x-pn-realaudio");
        MIME_TYPES.put("ra", "audio/x-pn-realaudio");
        MIME_TYPES.put("sdkm", "application/vnd.solent.sdkm+xml");
        MIME_TYPES.put("sdkd", "application/vnd.solent.sdkm+xml");
        MIME_TYPES.put("dtshd", "audio/vnd.dts.hd");
        MIME_TYPES.put("btif", "image/prs.btif");
        MIME_TYPES.put("scs", "application/scvp-cv-response");
        MIME_TYPES.put("car", "application/vnd.curl.car");
        MIME_TYPES.put("otf", "application/x-font-otf");
        MIME_TYPES.put("clkx", "application/vnd.crick.clicker");
        MIME_TYPES.put("xbd", "application/vnd.fujixerox.docuworks.binder");
        MIME_TYPES.put("ppm", "image/x-binary-pixmap");
        MIME_TYPES.put("wav", "audio/x-wav");
        MIME_TYPES.put("ssml", "application/ssml+xml");
        MIME_TYPES.put("p7b", "application/x-pkcs7-certificates");
        MIME_TYPES.put("spc", "application/x-pkcs7-certificates");
        MIME_TYPES.put("kia", "application/vnd.kidspiration");
        MIME_TYPES.put("rss", "application/rss+xml");
        MIME_TYPES.put("setreg", "application/set-registration-initiation");
        MIME_TYPES.put("qbo", "application/vnd.intu.qbo");
        MIME_TYPES.put("ras", "image/x-cmu-raster");
        MIME_TYPES.put("rar", "application/x-rar-compressed");
        MIME_TYPES.put("ogx", "application/ogg");
        MIME_TYPES.put("class", "application/java-vm");
        MIME_TYPES.put("smf", "application/vnd.stardivision.math");
        MIME_TYPES.put("atom", "application/atom+xml");
        MIME_TYPES.put("sit", "application/x-stuffit");
        MIME_TYPES.put("ez3", "application/vnd.ezpix-package");
        MIME_TYPES.put("mcurl", "text/vnd.curl.mcurl");
        MIME_TYPES.put("wmls", "text/vnd.wap.wmlscript");
        MIME_TYPES.put("srx", "application/sparql-results+xml");
        MIME_TYPES.put("wps", "application/vnd.ms-works");
        MIME_TYPES.put("wks", "application/vnd.ms-works");
        MIME_TYPES.put("wcm", "application/vnd.ms-works");
        MIME_TYPES.put("wdb", "application/vnd.ms-works");
        MIME_TYPES.put("vcs", "text/x-vcalendar");
        MIME_TYPES.put("ecma", "application/ecmascript");
        MIME_TYPES.put("curl", "text/vnd.curl");
        MIME_TYPES.put("std", "application/vnd.sun.xml.draw.template");
        MIME_TYPES.put("eot", "application/vnd.ms-fontobject");
        MIME_TYPES.put("fsc", "application/vnd.fsc.weblaunch");
        MIME_TYPES.put("tfm", "application/x-tex-tfm");
        MIME_TYPES.put("dra", "audio/vnd.dra");
        MIME_TYPES.put("mwf", "application/vnd.mfer");
        MIME_TYPES.put("hpid", "application/vnd.hp-hpid");
        MIME_TYPES.put("nml", "application/vnd.enliven");
        MIME_TYPES.put("hvp", "application/vnd.yamaha.hv-voice");
        MIME_TYPES.put("s", "text/x-asm");
        MIME_TYPES.put("asm", "text/x-asm");
        MIME_TYPES.put("mcd", "application/vnd.mcd");
        MIME_TYPES.put("mts", "model/vnd.mts");
        MIME_TYPES.put("igl", "application/vnd.igloader");
        MIME_TYPES.put("tao", "application/vnd.tao.intent-module-archive");
        MIME_TYPES.put("sgml", "text/sgml");
        MIME_TYPES.put("sgm", "text/sgml");
        MIME_TYPES.put("rmp", "audio/x-pn-realaudio-plugin");
        MIME_TYPES.put("xenc", "application/xenc+xml");
        MIME_TYPES.put("wpl", "application/vnd.ms-wpl");
        MIME_TYPES.put("dxf", "image/vnd.dxf");
        MIME_TYPES.put("pgm", "image/x-binary-graymap");
        MIME_TYPES.put("spot", "text/vnd.in3d.spot");
        MIME_TYPES.put("odt", "application/vnd.oasis.opendocument.text");
        MIME_TYPES.put("azs", "application/vnd.airzip.filesecure.azs");
        MIME_TYPES.put("es3", "application/vnd.eszigno3+xml");
        MIME_TYPES.put("et3", "application/vnd.eszigno3+xml");
        MIME_TYPES.put("dd2", "application/vnd.oma.dd2+xml");
        MIME_TYPES.put("semf", "application/vnd.semf");
        MIME_TYPES.put("semd", "application/vnd.semd");
        MIME_TYPES.put("pnm", "image/x-binary-anymap");
        MIME_TYPES.put("sema", "application/vnd.sema");
        MIME_TYPES.put("wma", "audio/x-ms-wma");
        MIME_TYPES.put("cww", "application/prs.cww");
        MIME_TYPES.put("scm", "application/vnd.lotus-screencam");
        MIME_TYPES.put("azf", "application/vnd.airzip.filesecure.azf");
        MIME_TYPES.put("oda", "application/oda");
        MIME_TYPES.put("dwg", "image/vnd.dwg");
        MIME_TYPES.put("h264", "video/h264");
        MIME_TYPES.put("hpgl", "application/vnd.hp-hpgl");
        MIME_TYPES.put("xpr", "application/vnd.is-xpr");
        MIME_TYPES.put("h263", "video/h263");
        MIME_TYPES.put("zip", "application/zip");
        MIME_TYPES.put("h261", "video/h261");
        MIME_TYPES.put("oti", "application/vnd.oasis.opendocument.image-template");
        MIME_TYPES.put("uoml", "application/vnd.uoml+xml");
        MIME_TYPES.put("xspf", "application/xspf+xml");
        MIME_TYPES.put("ppam", "application/vnd.ms-powerpoint.addin.macroenabled.12");
        MIME_TYPES.put("dtd", "application/xml-dtd");
        MIME_TYPES.put("gex", "application/vnd.geometry-explorer");
        MIME_TYPES.put("gre", "application/vnd.geometry-explorer");
        MIME_TYPES.put("dssc", "application/dssc+der");
    }

    /**
     * @param f File to detect content type.
     * @return Content type.
     */
    @Nullable public static String getContentType(File f) {
        try (FileInputStream is = new FileInputStream(f)) {
            byte[] data = new byte[Math.min((int)f.length(), PREVIEW_SIZE)];

            is.read(data);

            return getContentType(data, f.getName());
        }
        catch (IOException ignored) {
            return null;
        }
    }

    /**
     * @param data Bytes to detect content type.
     * @param name File name to detect content type by file name.
     * @return Content type.
     */
    @Nullable public static String getContentType(byte[] data, String name) {
        if (data == null)
            return null;

        byte[] hdr = new byte[PREVIEW_SIZE];

        System.arraycopy(data, 0, hdr, 0, Math.min(data.length, hdr.length));

        int c1 = hdr[0] & 0xff;
        int c2 = hdr[1] & 0xff;
        int c3 = hdr[2] & 0xff;
        int c4 = hdr[3] & 0xff;
        int c5 = hdr[4] & 0xff;
        int c6 = hdr[5] & 0xff;
        int c7 = hdr[6] & 0xff;
        int c8 = hdr[7] & 0xff;
        int c9 = hdr[8] & 0xff;
        int c10 = hdr[9] & 0xff;
        int c11 = hdr[10] & 0xff;

        if (c1 == 0xCA && c2 == 0xFE && c3 == 0xBA && c4 == 0xBE)
            return "application/java-vm";

        if (c1 == 0xD0 && c2 == 0xCF && c3 == 0x11 && c4 == 0xE0 && c5 == 0xA1 && c6 == 0xB1 && c7 == 0x1A && c8 == 0xE1) {
            // if the name is set then check if it can be validated by name, because it could be a xls or powerpoint
            String contentType = guessContentTypeFromName(name);

            if (contentType != null)
                return contentType;

            return "application/msword";
        }
        if (c1 == 0x25 && c2 == 0x50 && c3 == 0x44 && c4 == 0x46 && c5 == 0x2d && c6 == 0x31 && c7 == 0x2e)
            return "application/pdf";

        if (c1 == 0x38 && c2 == 0x42 && c3 == 0x50 && c4 == 0x53 && c5 == 0x00 && c6 == 0x01)
            return "image/photoshop";

        if (c1 == 0x25 && c2 == 0x21 && c3 == 0x50 && c4 == 0x53)
            return "application/postscript";

        if (c1 == 0xff && c2 == 0xfb && c3 == 0x30)
            return "audio/mp3";

        if (c1 == 0x49 && c2 == 0x44 && c3 == 0x33)
            return "audio/mp3";

        if (c1 == 0xAC && c2 == 0xED) {
            // next two bytes are version number, currently 0x00 0x05
            return "application/x-java-serialized-object";
        }

        if (c1 == '<') {
            if (c2 == '!' ||
                ((c2 == 'h' && (c3 == 't' && c4 == 'm' && c5 == 'l' || c3 == 'e' && c4 == 'a' && c5 == 'd') || (c2 == 'b' && c3 == 'o' && c4 == 'd' && c5 == 'y'))) ||
                ((c2 == 'H' && (c3 == 'T' && c4 == 'M' && c5 == 'L' || c3 == 'E' && c4 == 'A' && c5 == 'D') || (c2 == 'B' && c3 == 'O' && c4 == 'D' && c5 == 'Y'))))
                return "text/html";

            if (c2 == '?' && c3 == 'x' && c4 == 'm' && c5 == 'l' && c6 == ' ')
                return "application/xml";
        }

        // big and little endian UTF-16 encodings, with byte order mark
        if (c1 == 0xfe && c2 == 0xff) {
            if (c3 == 0 && c4 == '<' && c5 == 0 && c6 == '?' && c7 == 0 && c8 == 'x')
                return "application/xml";
        }

        if (c1 == 0xff && c2 == 0xfe) {
            if (c3 == '<' && c4 == 0 && c5 == '?' && c6 == 0 && c7 == 'x' && c8 == 0)
                return "application/xml";
        }

        if (c1 == 'B' && c2 == 'M')
            return "image/bmp";

        if (c1 == 0x49 && c2 == 0x49 && c3 == 0x2a && c4 == 0x00)
            return "image/tiff";

        if (c1 == 0x4D && c2 == 0x4D && c3 == 0x00 && c4 == 0x2a)
            return "image/tiff";

        if (c1 == 'G' && c2 == 'I' && c3 == 'F' && c4 == '8')
            return "image/gif";

        if (c1 == '#' && c2 == 'd' && c3 == 'e' && c4 == 'f')
            return "image/x-bitmap";

        if (c1 == '!' && c2 == ' ' && c3 == 'X' && c4 == 'P' && c5 == 'M' && c6 == '2')
            return "image/x-pixmap";

        if (c1 == 137 && c2 == 80 && c3 == 78 && c4 == 71 && c5 == 13 && c6 == 10 && c7 == 26 && c8 == 10)
            return "image/png";

        if (c1 == 0xFF && c2 == 0xD8 && c3 == 0xFF) {
            if (c4 == 0xE0)
                return "image/jpeg";

            // File format used by digital cameras to store images. Exif Format can be read by any application supporting JPEG. Exif Spec can be found at:
            // http://www.pima.net/standards/it10/PIMA15740/Exif_2-1.PDF
            if ((c4 == 0xE1) && (c7 == 'E' && c8 == 'x' && c9 == 'i' && c10 == 'f' && c11 == 0))
                return "image/jpeg";

            if (c4 == 0xEE)
                return "image/jpg";
        }

        // According to http://www.opendesign.com/files/guestdownloads/OpenDesign_Specification_for_.dwg_files.pdf
        // first 6 bytes are of type "AC1018" (for example) and the next 5 bytes are 0x00.
        if ((c1 == 0x41 && c2 == 0x43) && (c7 == 0x00 && c8 == 0x00 && c9 == 0x00 && c10 == 0x00 && c11 == 0x00))
            return "application/acad";

        if (c1 == 0x2E && c2 == 0x73 && c3 == 0x6E && c4 == 0x64)
            return "audio/basic"; // .au format, big endian

        if (c1 == 0x64 && c2 == 0x6E && c3 == 0x73 && c4 == 0x2E)
            return "audio/basic"; // .au format, little endian

        // I don't know if this is official but evidence suggests that .wav files start with "RIFF" - brown
        if (c1 == 'R' && c2 == 'I' && c3 == 'F' && c4 == 'F')
            return "audio/x-wav";

        if (c1 == 'P' && c2 == 'K') {
            // its application/zip but this could be a open office thing if name is given
            String contentType = guessContentTypeFromName(name);
            if (contentType != null)
                return contentType;
            return "application/zip";
        }
        return guessContentTypeFromName(name);
    }

    /**
     * @param name File name to detect content type by file name.
     * @return Content type.
     */
    @Nullable public static String guessContentTypeFromName(String name) {
        if (name == null)
            return null;

        int lastIdx = name.lastIndexOf('.');

        if (lastIdx != -1) {
            String extension = name.substring(lastIdx + 1).toLowerCase();

            return MIME_TYPES.get(extension);
        }

        return null;
    }
}
