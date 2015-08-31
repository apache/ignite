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
    private static final Map<String, String> mimeTypes = U.newHashMap(810);

    static {
        mimeTypes.put("mseed", "application/vnd.fdsn.mseed");
        mimeTypes.put("vsf", "application/vnd.vsf");
        mimeTypes.put("cmdf", "chemical/x-cmdf");
        mimeTypes.put("mxs", "application/vnd.triscape.mxs");
        mimeTypes.put("m4v", "video/x-m4v");
        mimeTypes.put("oga", "audio/ogg");
        mimeTypes.put("ogg", "audio/ogg");
        mimeTypes.put("spx", "audio/ogg");
        mimeTypes.put("shf", "application/shf+xml");
        mimeTypes.put("jisp", "application/vnd.jisp");
        mimeTypes.put("sgl", "application/vnd.stardivision.writer-global");
        mimeTypes.put("gxt", "application/vnd.geonext");
        mimeTypes.put("mp4s", "application/mp4");
        mimeTypes.put("smi", "application/smil+xml");
        mimeTypes.put("smil", "application/smil+xml");
        mimeTypes.put("xop", "application/xop+xml");
        mimeTypes.put("spl", "application/x-futuresplash");
        mimeTypes.put("spq", "application/scvp-vp-request");
        mimeTypes.put("bh2", "application/vnd.fujitsu.oasysprs");
        mimeTypes.put("xpm", "image/x-xpixmap");
        mimeTypes.put("gdl", "model/vnd.gdl");
        mimeTypes.put("dotx", "application/vnd.openxmlformats-officedocument.wordprocessingml.template");
        mimeTypes.put("ser", "application/java-serialized-object");
        mimeTypes.put("ghf", "application/vnd.groove-help");
        mimeTypes.put("mc1", "application/vnd.medcalcdata");
        mimeTypes.put("cdy", "application/vnd.cinderella");
        mimeTypes.put("nns", "application/vnd.noblenet-sealer");
        mimeTypes.put("msty", "application/vnd.muvee.style");
        mimeTypes.put("aas", "application/x-authorware-seg");
        mimeTypes.put("p", "text/x-pascal");
        mimeTypes.put("pas", "text/x-pascal");
        mimeTypes.put("rdz", "application/vnd.data-vision.rdz");
        mimeTypes.put("js", "text/javascript");
        mimeTypes.put("fzs", "application/vnd.fuzzysheet");
        mimeTypes.put("csml", "chemical/x-csml");
        mimeTypes.put("psf", "application/x-font-linux-psf");
        mimeTypes.put("afp", "application/vnd.ibm.modcap");
        mimeTypes.put("listafp", "application/vnd.ibm.modcap");
        mimeTypes.put("list3820", "application/vnd.ibm.modcap");
        mimeTypes.put("qt", "video/quicktime");
        mimeTypes.put("mov", "video/quicktime");
        mimeTypes.put("fly", "text/vnd.fly");
        mimeTypes.put("rlc", "image/vnd.fujixerox.edmics-rlc");
        mimeTypes.put("sxi", "application/vnd.sun.xml.impress");
        mimeTypes.put("vor", "application/vnd.stardivision.writer");
        mimeTypes.put("gac", "application/vnd.groove-account");
        mimeTypes.put("sldm", "application/vnd.ms-powerpoint.slide.macroenabled.12");
        mimeTypes.put("pgn", "application/x-chess-pgn");
        mimeTypes.put("zaz", "application/vnd.zzazz.deck+xml");
        mimeTypes.put("ccxml", "application/ccxml+xml");
        mimeTypes.put("csh", "application/x-csh");
        mimeTypes.put("fvt", "video/vnd.fvt");
        mimeTypes.put("grv", "application/vnd.groove-injector");
        mimeTypes.put("scurl", "text/vnd.curl.scurl");
        mimeTypes.put("oa3", "application/vnd.fujitsu.oasys3");
        mimeTypes.put("oa2", "application/vnd.fujitsu.oasys2");
        mimeTypes.put("rgb", "image/x-rgb");
        mimeTypes.put("pfr", "application/font-tdpfr");
        mimeTypes.put("pbd", "application/vnd.powerbuilder6");
        mimeTypes.put("psd", "image/vnd.adobe.photoshop");
        mimeTypes.put("fh", "image/x-freehand");
        mimeTypes.put("fhc", "image/x-freehand");
        mimeTypes.put("fh4", "image/x-freehand");
        mimeTypes.put("fh5", "image/x-freehand");
        mimeTypes.put("fh7", "image/x-freehand");
        mimeTypes.put("doc", "application/msword");
        mimeTypes.put("dot", "application/msword");
        mimeTypes.put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        mimeTypes.put("plc", "application/vnd.mobius.plc");
        mimeTypes.put("rnc", "application/relax-ng-compact-syntax");
        mimeTypes.put("rpss", "application/vnd.nokia.radio-presets");
        mimeTypes.put("nlu", "application/vnd.neurolanguage.nlu");
        mimeTypes.put("pcl", "application/vnd.hp-pcl");
        mimeTypes.put("uu", "text/x-uuencode");
        mimeTypes.put("ami", "application/vnd.amiga.ami");
        mimeTypes.put("viv", "video/vnd.vivo");
        mimeTypes.put("mp4a", "audio/mp4");
        mimeTypes.put("vis", "application/vnd.visionary");
        mimeTypes.put("asc", "application/pgp-signature");
        mimeTypes.put("sig", "application/pgp-signature");
        mimeTypes.put("karbon", "application/vnd.kde.karbon");
        mimeTypes.put("htke", "application/vnd.kenameaapp");
        mimeTypes.put("nnd", "application/vnd.noblenet-directory");
        mimeTypes.put("xlsm", "application/vnd.ms-excel.sheet.macroenabled.12");
        mimeTypes.put("ez2", "application/vnd.ezpix-album");
        mimeTypes.put("otm", "application/vnd.oasis.opendocument.text-master");
        mimeTypes.put("f", "text/x-fortran");
        mimeTypes.put("for", "text/x-fortran");
        mimeTypes.put("f77", "text/x-fortran");
        mimeTypes.put("f90", "text/x-fortran");
        mimeTypes.put("apr", "application/vnd.lotus-approach");
        mimeTypes.put("mid", "audio/midi");
        mimeTypes.put("midi", "audio/midi");
        mimeTypes.put("kar", "audio/midi");
        mimeTypes.put("rmi", "audio/midi");
        mimeTypes.put("tmo", "application/vnd.tmobile-livetv");
        mimeTypes.put("pya", "audio/vnd.ms-playready.media.pya");
        mimeTypes.put("cgm", "image/cgm");
        mimeTypes.put("uri", "text/uri-list");
        mimeTypes.put("uris", "text/uri-list");
        mimeTypes.put("urls", "text/uri-list");
        mimeTypes.put("ipk", "application/vnd.shana.informed.package");
        mimeTypes.put("clkk", "application/vnd.crick.clicker.keyboard");
        mimeTypes.put("mbox", "application/mbox");
        mimeTypes.put("unityweb", "application/vnd.unity");
        mimeTypes.put("joda", "application/vnd.joost.joda-archive");
        mimeTypes.put("kwd", "application/vnd.kde.kword");
        mimeTypes.put("kwt", "application/vnd.kde.kword");
        mimeTypes.put("mpy", "application/vnd.ibm.minipay");
        mimeTypes.put("mpeg", "video/mpeg");
        mimeTypes.put("mpg", "video/mpeg");
        mimeTypes.put("mpe", "video/mpeg");
        mimeTypes.put("m1v", "video/mpeg");
        mimeTypes.put("m2v", "video/mpeg");
        mimeTypes.put("asf", "video/x-ms-asf");
        mimeTypes.put("asx", "video/x-ms-asf");
        mimeTypes.put("jad", "text/vnd.sun.j2me.app-descriptor");
        mimeTypes.put("rm", "application/vnd.rn-realmedia");
        mimeTypes.put("xer", "application/patch-ops-error+xml");
        mimeTypes.put("xml", "application/xml");
        mimeTypes.put("xsl", "application/xml");
        mimeTypes.put("icc", "application/vnd.iccprofile");
        mimeTypes.put("icm", "application/vnd.iccprofile");
        mimeTypes.put("mmr", "image/vnd.fujixerox.edmics-mmr");
        mimeTypes.put("hvd", "application/vnd.yamaha.hv-dic");
        mimeTypes.put("mbk", "application/vnd.mobius.mbk");
        mimeTypes.put("123", "application/vnd.lotus-1-2-3");
        mimeTypes.put("fe_launch", "application/vnd.denovo.fcselayout-link");
        mimeTypes.put("setpay", "application/set-payment-initiation");
        mimeTypes.put("sdc", "application/vnd.stardivision.calc");
        mimeTypes.put("swi", "application/vnd.aristanetworks.swi");
        mimeTypes.put("g3", "image/g3fax");
        mimeTypes.put("cil", "application/vnd.ms-artgalry");
        mimeTypes.put("vcg", "application/vnd.groove-vcard");
        mimeTypes.put("chm", "application/vnd.ms-htmlhelp");
        mimeTypes.put("grxml", "application/srgs+xml");
        mimeTypes.put("ext", "application/vnd.novadigm.ext");
        mimeTypes.put("opf", "application/oebps-package+xml");
        mimeTypes.put("gtm", "application/vnd.groove-tool-message");
        mimeTypes.put("au", "audio/basic");
        mimeTypes.put("snd", "audio/basic");
        mimeTypes.put("ppsm", "application/vnd.ms-powerpoint.slideshow.macroenabled.12");
        mimeTypes.put("wbxml", "application/vnd.wap.wbxml");
        mimeTypes.put("oxt", "application/vnd.openofficeorg.extension");
        mimeTypes.put("davmount", "application/davmount+xml");
        mimeTypes.put("ivu", "application/vnd.immervision-ivu");
        mimeTypes.put("wrl", "model/vrml");
        mimeTypes.put("vrml", "model/vrml");
        mimeTypes.put("p7m", "application/pkcs7-mime");
        mimeTypes.put("p7c", "application/pkcs7-mime");
        mimeTypes.put("ppt", "application/vnd.ms-powerpoint");
        mimeTypes.put("pps", "application/vnd.ms-powerpoint");
        mimeTypes.put("pot", "application/vnd.ms-powerpoint");
        mimeTypes.put("ivp", "application/vnd.immervision-ivp");
        mimeTypes.put("movie", "video/x-sgi-movie");
        mimeTypes.put("ecelp4800", "audio/vnd.nuera.ecelp4800");
        mimeTypes.put("tpl", "application/vnd.groove-tool-template");
        mimeTypes.put("fnc", "application/vnd.frogans.fnc");
        mimeTypes.put("wax", "audio/x-ms-wax");
        mimeTypes.put("3gp", "video/3gpp");
        mimeTypes.put("ppd", "application/vnd.cups-ppd");
        mimeTypes.put("mmf", "application/vnd.smaf");
        mimeTypes.put("exe", "application/x-msdownload");
        mimeTypes.put("dll", "application/x-msdownload");
        mimeTypes.put("com", "application/x-msdownload");
        mimeTypes.put("bat", "application/x-msdownload");
        mimeTypes.put("msi", "application/x-msdownload");
        mimeTypes.put("skp", "application/vnd.koan");
        mimeTypes.put("skd", "application/vnd.koan");
        mimeTypes.put("skt", "application/vnd.koan");
        mimeTypes.put("skm", "application/vnd.koan");
        mimeTypes.put("ice", "x-conference/x-cooltalk");
        mimeTypes.put("emma", "application/emma+xml");
        mimeTypes.put("odc", "application/vnd.oasis.opendocument.chart");
        mimeTypes.put("atomcat", "application/atomcat+xml");
        mimeTypes.put("onetoc", "application/onenote");
        mimeTypes.put("onetoc2", "application/onenote");
        mimeTypes.put("onetmp", "application/onenote");
        mimeTypes.put("onepkg", "application/onenote");
        mimeTypes.put("otp", "application/vnd.oasis.opendocument.presentation-template");
        mimeTypes.put("irm", "application/vnd.ibm.rights-management");
        mimeTypes.put("texinfo", "application/x-texinfo");
        mimeTypes.put("texi", "application/x-texinfo");
        mimeTypes.put("spp", "application/scvp-vp-response");
        mimeTypes.put("xif", "image/vnd.xiff");
        mimeTypes.put("sitx", "application/x-stuffitx");
        mimeTypes.put("eol", "audio/vnd.digital-winds");
        mimeTypes.put("c4g", "application/vnd.clonk.c4group");
        mimeTypes.put("c4d", "application/vnd.clonk.c4group");
        mimeTypes.put("c4f", "application/vnd.clonk.c4group");
        mimeTypes.put("c4p", "application/vnd.clonk.c4group");
        mimeTypes.put("c4u", "application/vnd.clonk.c4group");
        mimeTypes.put("flo", "application/vnd.micrografx.flo");
        mimeTypes.put("svg", "image/svg+xml");
        mimeTypes.put("svgz", "image/svg+xml");
        mimeTypes.put("xsm", "application/vnd.syncml+xml");
        mimeTypes.put("hdf", "application/x-hdf");
        mimeTypes.put("cml", "chemical/x-cml");
        mimeTypes.put("atx", "application/vnd.antix.game-component");
        mimeTypes.put("flv", "video/x-flv");
        mimeTypes.put("m3u8", "application/vnd.apple.mpegurl");
        mimeTypes.put("wbs", "application/vnd.criticaltools.wbs+xml");
        mimeTypes.put("bz2", "application/x-bzip2");
        mimeTypes.put("boz", "application/x-bzip2");
        mimeTypes.put("plf", "application/vnd.pocketlearn");
        mimeTypes.put("lbe", "application/vnd.llamagraphics.life-balance.exchange+xml");
        mimeTypes.put("vcd", "application/x-cdlink");
        mimeTypes.put("sxg", "application/vnd.sun.xml.writer.global");
        mimeTypes.put("fli", "video/x-fli");
        mimeTypes.put("wsdl", "application/wsdl+xml");
        mimeTypes.put("slt", "application/vnd.epson.salt");
        mimeTypes.put("lwp", "application/vnd.lotus-wordpro");
        mimeTypes.put("ddd", "application/vnd.fujixerox.ddd");
        mimeTypes.put("xfdf", "application/vnd.adobe.xfdf");
        mimeTypes.put("portpkg", "application/vnd.macports.portpkg");
        mimeTypes.put("pfa", "application/x-font-type1");
        mimeTypes.put("pfb", "application/x-font-type1");
        mimeTypes.put("pfm", "application/x-font-type1");
        mimeTypes.put("afm", "application/x-font-type1");
        mimeTypes.put("nc", "application/x-netcdf");
        mimeTypes.put("cdf", "application/x-netcdf");
        mimeTypes.put("mpm", "application/vnd.blueice.multipass");
        mimeTypes.put("sis", "application/vnd.symbian.install");
        mimeTypes.put("sisx", "application/vnd.symbian.install");
        mimeTypes.put("ustar", "application/x-ustar");
        mimeTypes.put("rpst", "application/vnd.nokia.radio-preset");
        mimeTypes.put("eml", "message/rfc822");
        mimeTypes.put("mime", "message/rfc822");
        mimeTypes.put("g2w", "application/vnd.geoplan");
        mimeTypes.put("ma", "application/mathematica");
        mimeTypes.put("nb", "application/mathematica");
        mimeTypes.put("mb", "application/mathematica");
        mimeTypes.put("csv", "text/csv");
        mimeTypes.put("css", "text/css");
        mimeTypes.put("wvx", "video/x-ms-wvx");
        mimeTypes.put("jlt", "application/vnd.hp-jlyt");
        mimeTypes.put("vcx", "application/vnd.vcx");
        mimeTypes.put("html", "text/html");
        mimeTypes.put("htm", "text/html");
        mimeTypes.put("docm", "application/vnd.ms-word.document.macroenabled.12");
        mimeTypes.put("xdssc", "application/dssc+xml");
        mimeTypes.put("pbm", "image/x-portable-bitmap");
        mimeTypes.put("fdf", "application/vnd.fdf");
        mimeTypes.put("ggt", "application/vnd.geogebra.tool");
        mimeTypes.put("cii", "application/vnd.anser-web-certificate-issue-initiation");
        mimeTypes.put("atomsvc", "application/atomsvc+xml");
        mimeTypes.put("stw", "application/vnd.sun.xml.writer.template");
        mimeTypes.put("vtu", "model/vnd.vtu");
        mimeTypes.put("latex", "application/x-latex");
        mimeTypes.put("cat", "application/vnd.ms-pki.seccat");
        mimeTypes.put("odf", "application/vnd.oasis.opendocument.formula");
        mimeTypes.put("trm", "application/x-msterminal");
        mimeTypes.put("pptm", "application/vnd.ms-powerpoint.presentation.macroenabled.12");
        mimeTypes.put("stl", "application/vnd.ms-pki.stl");
        mimeTypes.put("ltf", "application/vnd.frogans.ltf");
        mimeTypes.put("obd", "application/x-msbinder");
        mimeTypes.put("sda", "application/vnd.stardivision.draw");
        mimeTypes.put("org", "application/vnd.lotus-organizer");
        mimeTypes.put("ftc", "application/vnd.fluxtime.clip");
        mimeTypes.put("rcprofile", "application/vnd.ipunplugged.rcprofile");
        mimeTypes.put("cmx", "image/x-cmx");
        mimeTypes.put("cif", "chemical/x-cif");
        mimeTypes.put("rp9", "application/vnd.cloanto.rp9");
        mimeTypes.put("pvb", "application/vnd.3gpp.pic-bw-var");
        mimeTypes.put("aw", "application/applixware");
        mimeTypes.put("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation");
        mimeTypes.put("rld", "application/resource-lists-diff+xml");
        mimeTypes.put("xar", "application/vnd.xara");
        mimeTypes.put("ecelp7470", "audio/vnd.nuera.ecelp7470");
        mimeTypes.put("xls", "application/vnd.ms-excel");
        mimeTypes.put("xlm", "application/vnd.ms-excel");
        mimeTypes.put("xla", "application/vnd.ms-excel");
        mimeTypes.put("xlc", "application/vnd.ms-excel");
        mimeTypes.put("xlt", "application/vnd.ms-excel");
        mimeTypes.put("xlw", "application/vnd.ms-excel");
        mimeTypes.put("txf", "application/vnd.mobius.txf");
        mimeTypes.put("prc", "application/x-mobipocket-ebook");
        mimeTypes.put("mobi", "application/x-mobipocket-ebook");
        mimeTypes.put("swf", "application/x-shockwave-flash");
        mimeTypes.put("sfs", "application/vnd.spotfire.sfs");
        mimeTypes.put("dp", "application/vnd.osgi.dp");
        mimeTypes.put("potx", "application/vnd.openxmlformats-officedocument.presentationml.template");
        mimeTypes.put("efif", "application/vnd.picsel");
        mimeTypes.put("pdf", "application/pdf");
        mimeTypes.put("dsc", "text/prs.lines.tag");
        mimeTypes.put("mathml", "application/mathml+xml");
        mimeTypes.put("bed", "application/vnd.realvnc.bed");
        mimeTypes.put("bcpio", "application/x-bcpio");
        mimeTypes.put("shar", "application/x-shar");
        mimeTypes.put("xdm", "application/vnd.syncml.dm+xml");
        mimeTypes.put("teacher", "application/vnd.smart.teacher");
        mimeTypes.put("sldx", "application/vnd.openxmlformats-officedocument.presentationml.slide");
        mimeTypes.put("stf", "application/vnd.wt.stf");
        mimeTypes.put("odb", "application/vnd.oasis.opendocument.database");
        mimeTypes.put("bdf", "application/x-font-bdf");
        mimeTypes.put("tcap", "application/vnd.3gpp2.tcap");
        mimeTypes.put("fm", "application/vnd.framemaker");
        mimeTypes.put("frame", "application/vnd.framemaker");
        mimeTypes.put("maker", "application/vnd.framemaker");
        mimeTypes.put("book", "application/vnd.framemaker");
        mimeTypes.put("src", "application/x-wais-source");
        mimeTypes.put("rep", "application/vnd.businessobjects");
        mimeTypes.put("dna", "application/vnd.dna");
        mimeTypes.put("jpgv", "video/jpeg");
        mimeTypes.put("ez", "application/andrew-inset");
        mimeTypes.put("vxml", "application/voicexml+xml");
        mimeTypes.put("x3d", "application/vnd.hzn-3d-crossword");
        mimeTypes.put("dxp", "application/vnd.spotfire.dxp");
        mimeTypes.put("dvi", "application/x-dvi");
        mimeTypes.put("f4v", "video/x-f4v");
        mimeTypes.put("fg5", "application/vnd.fujitsu.oasysgp");
        mimeTypes.put("clp", "application/x-msclip");
        mimeTypes.put("acu", "application/vnd.acucobol");
        mimeTypes.put("ksp", "application/vnd.kde.kspread");
        mimeTypes.put("rms", "application/vnd.jcp.javame.midlet-rms");
        mimeTypes.put("lvp", "audio/vnd.lucent.voice");
        mimeTypes.put("jpm", "video/jpm");
        mimeTypes.put("jpgm", "video/jpm");
        mimeTypes.put("pcx", "image/x-pcx");
        mimeTypes.put("sxm", "application/vnd.sun.xml.math");
        mimeTypes.put("g3w", "application/vnd.geospace");
        mimeTypes.put("ngdat", "application/vnd.nokia.n-gage.data");
        mimeTypes.put("clkp", "application/vnd.crick.clicker.palette");
        mimeTypes.put("osfpvg", "application/vnd.yamaha.openscoreformat.osfpvg+xml");
        mimeTypes.put("aep", "application/vnd.audiograph");
        mimeTypes.put("wqd", "application/vnd.wqd");
        mimeTypes.put("mdi", "image/vnd.ms-modi");
        mimeTypes.put("ecelp9600", "audio/vnd.nuera.ecelp9600");
        mimeTypes.put("fig", "application/x-xfig");
        mimeTypes.put("p7r", "application/x-pkcs7-certreqresp");
        mimeTypes.put("jar", "application/java-archive");
        mimeTypes.put("ief", "image/ief");
        mimeTypes.put("hvs", "application/vnd.yamaha.hv-script");
        mimeTypes.put("cdx", "chemical/x-cdx");
        mimeTypes.put("abw", "application/x-abiword");
        mimeTypes.put("deb", "application/x-debian-package");
        mimeTypes.put("udeb", "application/x-debian-package");
        mimeTypes.put("djvu", "image/vnd.djvu");
        mimeTypes.put("djv", "image/vnd.djvu");
        mimeTypes.put("wml", "text/vnd.wap.wml");
        mimeTypes.put("xhtml", "application/xhtml+xml");
        mimeTypes.put("xht", "application/xhtml+xml");
        mimeTypes.put("tpt", "application/vnd.trid.tpt");
        mimeTypes.put("wmz", "application/x-ms-wmz");
        mimeTypes.put("rtx", "text/richtext");
        mimeTypes.put("wspolicy", "application/wspolicy+xml");
        mimeTypes.put("odg", "application/vnd.oasis.opendocument.graphics");
        mimeTypes.put("res", "application/x-dtbresource+xml");
        mimeTypes.put("xbm", "image/x-xbitmap");
        mimeTypes.put("zir", "application/vnd.zul");
        mimeTypes.put("zirz", "application/vnd.zul");
        mimeTypes.put("cdkey", "application/vnd.mediastation.cdkey");
        mimeTypes.put("wmd", "application/x-ms-wmd");
        mimeTypes.put("ogv", "video/ogg");
        mimeTypes.put("scq", "application/scvp-cv-request");
        mimeTypes.put("sfd-hdstx", "application/vnd.hydrostatix.sof-data");
        mimeTypes.put("igx", "application/vnd.micrografx.igx");
        mimeTypes.put("xps", "application/vnd.ms-xpsdocument");
        mimeTypes.put("xdw", "application/vnd.fujixerox.docuworks");
        mimeTypes.put("kfo", "application/vnd.kde.kformula");
        mimeTypes.put("chrt", "application/vnd.kde.kchart");
        mimeTypes.put("sdp", "application/sdp");
        mimeTypes.put("oth", "application/vnd.oasis.opendocument.text-web");
        mimeTypes.put("3g2", "video/3gpp2");
        mimeTypes.put("utz", "application/vnd.uiq.theme");
        mimeTypes.put("mus", "application/vnd.musician");
        mimeTypes.put("wpd", "application/vnd.wordperfect");
        mimeTypes.put("oas", "application/vnd.fujitsu.oasys");
        mimeTypes.put("pic", "image/x-pict");
        mimeTypes.put("pct", "image/x-pict");
        mimeTypes.put("wmx", "video/x-ms-wmx");
        mimeTypes.put("p10", "application/pkcs10");
        mimeTypes.put("wmv", "video/x-ms-wmv");
        mimeTypes.put("xfdl", "application/vnd.xfdl");
        mimeTypes.put("pgp", "application/pgp-encrypted");
        mimeTypes.put("rs", "application/rls-services+xml");
        mimeTypes.put("cpt", "application/mac-compactpro");
        mimeTypes.put("gmx", "application/vnd.gmx");
        mimeTypes.put("potm", "application/vnd.ms-powerpoint.template.macroenabled.12");
        mimeTypes.put("iif", "application/vnd.shana.informed.interchange");
        mimeTypes.put("ace", "application/x-ace-compressed");
        mimeTypes.put("pcurl", "application/vnd.curl.pcurl");
        mimeTypes.put("ods", "application/vnd.oasis.opendocument.spreadsheet");
        mimeTypes.put("vsd", "application/vnd.visio");
        mimeTypes.put("vst", "application/vnd.visio");
        mimeTypes.put("vss", "application/vnd.visio");
        mimeTypes.put("vsw", "application/vnd.visio");
        mimeTypes.put("ifm", "application/vnd.shana.informed.formdata");
        mimeTypes.put("fbs", "image/vnd.fastbidsheet");
        mimeTypes.put("qxd", "application/vnd.quark.quarkxpress");
        mimeTypes.put("qxt", "application/vnd.quark.quarkxpress");
        mimeTypes.put("qwd", "application/vnd.quark.quarkxpress");
        mimeTypes.put("qwt", "application/vnd.quark.quarkxpress");
        mimeTypes.put("qxl", "application/vnd.quark.quarkxpress");
        mimeTypes.put("qxb", "application/vnd.quark.quarkxpress");
        mimeTypes.put("epub", "application/epub+zip");
        mimeTypes.put("crl", "application/pkix-crl");
        mimeTypes.put("nnw", "application/vnd.noblenet-web");
        mimeTypes.put("nbp", "application/vnd.wolfram.player");
        mimeTypes.put("plb", "application/vnd.3gpp.pic-bw-large");
        mimeTypes.put("itp", "application/vnd.shana.informed.formtemplate");
        mimeTypes.put("snf", "application/x-font-snf");
        mimeTypes.put("str", "application/vnd.pg.format");
        mimeTypes.put("wtb", "application/vnd.webturbo");
        mimeTypes.put("osf", "application/vnd.yamaha.openscoreformat");
        mimeTypes.put("xltx", "application/vnd.openxmlformats-officedocument.spreadsheetml.template");
        mimeTypes.put("pre", "application/vnd.lotus-freelance");
        mimeTypes.put("clkt", "application/vnd.crick.clicker.template");
        mimeTypes.put("hbci", "application/vnd.hbci");
        mimeTypes.put("dwf", "model/vnd.dwf");
        mimeTypes.put("igs", "model/iges");
        mimeTypes.put("iges", "model/iges");
        mimeTypes.put("ktz", "application/vnd.kahootz");
        mimeTypes.put("ktr", "application/vnd.kahootz");
        mimeTypes.put("n-gage", "application/vnd.nokia.n-gage.symbian.install");
        mimeTypes.put("otg", "application/vnd.oasis.opendocument.graphics-template");
        mimeTypes.put("rsd", "application/rsd+xml");
        mimeTypes.put("hlp", "application/winhlp");
        mimeTypes.put("azw", "application/vnd.amazon.ebook");
        mimeTypes.put("msf", "application/vnd.epson.msf");
        mimeTypes.put("mp4", "video/mp4");
        mimeTypes.put("mp4v", "video/mp4");
        mimeTypes.put("mpg4", "video/mp4");
        mimeTypes.put("cod", "application/vnd.rim.cod");
        mimeTypes.put("st", "application/vnd.sailingtracker.track");
        mimeTypes.put("odi", "application/vnd.oasis.opendocument.image");
        mimeTypes.put("tra", "application/vnd.trueapp");
        mimeTypes.put("wm", "video/x-ms-wm");
        mimeTypes.put("bin", "application/octet-stream");
        mimeTypes.put("dms", "application/octet-stream");
        mimeTypes.put("lha", "application/octet-stream");
        mimeTypes.put("lrf", "application/octet-stream");
        mimeTypes.put("lzh", "application/octet-stream");
        mimeTypes.put("so", "application/octet-stream");
        mimeTypes.put("iso", "application/octet-stream");
        mimeTypes.put("dmg", "application/octet-stream");
        mimeTypes.put("dist", "application/octet-stream");
        mimeTypes.put("distz", "application/octet-stream");
        mimeTypes.put("pkg", "application/octet-stream");
        mimeTypes.put("bpk", "application/octet-stream");
        mimeTypes.put("dump", "application/octet-stream");
        mimeTypes.put("elc", "application/octet-stream");
        mimeTypes.put("deploy", "application/octet-stream");
        mimeTypes.put("tsv", "text/tab-separated-values");
        mimeTypes.put("esf", "application/vnd.epson.esf");
        mimeTypes.put("p7s", "application/pkcs7-signature");
        mimeTypes.put("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        mimeTypes.put("geo", "application/vnd.dynageo");
        mimeTypes.put("mmd", "application/vnd.chipnuts.karaoke-mmd");
        mimeTypes.put("mxml", "application/xv+xml");
        mimeTypes.put("xhvml", "application/xv+xml");
        mimeTypes.put("xvml", "application/xv+xml");
        mimeTypes.put("xvm", "application/xv+xml");
        mimeTypes.put("nsf", "application/vnd.lotus-notes");
        mimeTypes.put("crd", "application/x-mscardfile");
        mimeTypes.put("p12", "application/x-pkcs12");
        mimeTypes.put("pfx", "application/x-pkcs12");
        mimeTypes.put("c", "text/x-c");
        mimeTypes.put("cc", "text/x-c");
        mimeTypes.put("cxx", "text/x-c");
        mimeTypes.put("cpp", "text/x-c");
        mimeTypes.put("h", "text/x-c");
        mimeTypes.put("hh", "text/x-c");
        mimeTypes.put("dic", "text/x-c");
        mimeTypes.put("cla", "application/vnd.claymore");
        mimeTypes.put("avi", "video/x-msvideo");
        mimeTypes.put("edx", "application/vnd.novadigm.edx");
        mimeTypes.put("gph", "application/vnd.flographit");
        mimeTypes.put("rq", "application/sparql-query");
        mimeTypes.put("xdp", "application/vnd.adobe.xdp+xml");
        mimeTypes.put("umj", "application/vnd.umajin");
        mimeTypes.put("pclxl", "application/vnd.hp-pclxl");
        mimeTypes.put("edm", "application/vnd.novadigm.edm");
        mimeTypes.put("gif", "image/gif");
        mimeTypes.put("jpeg", "image/jpeg");
        mimeTypes.put("jpg", "image/jpeg");
        mimeTypes.put("jpe", "image/jpeg");
        mimeTypes.put("adp", "audio/adpcm");
        mimeTypes.put("txt", "text/plain");
        mimeTypes.put("text", "text/plain");
        mimeTypes.put("conf", "text/plain");
        mimeTypes.put("def", "text/plain");
        mimeTypes.put("list", "text/plain");
        mimeTypes.put("log", "text/plain");
        mimeTypes.put("in", "text/plain");
        mimeTypes.put("gsf", "application/x-font-ghostscript");
        mimeTypes.put("aab", "application/x-authorware-bin");
        mimeTypes.put("x32", "application/x-authorware-bin");
        mimeTypes.put("u32", "application/x-authorware-bin");
        mimeTypes.put("vox", "application/x-authorware-bin");
        mimeTypes.put("ott", "application/vnd.oasis.opendocument.text-template");
        mimeTypes.put("kmz", "application/vnd.google-earth.kmz");
        mimeTypes.put("etx", "text/x-setext");
        mimeTypes.put("sv4crc", "application/x-sv4crc");
        mimeTypes.put("xlam", "application/vnd.ms-excel.addin.macroenabled.12");
        mimeTypes.put("pcf", "application/x-font-pcf");
        mimeTypes.put("torrent", "application/x-bittorrent");
        mimeTypes.put("twd", "application/vnd.simtech-mindmapper");
        mimeTypes.put("twds", "application/vnd.simtech-mindmapper");
        mimeTypes.put("qam", "application/vnd.epson.quickanime");
        mimeTypes.put("bdm", "application/vnd.syncml.dm+wbxml");
        mimeTypes.put("3dml", "text/vnd.in3d.3dml");
        mimeTypes.put("saf", "application/vnd.yamaha.smaf-audio");
        mimeTypes.put("ncx", "application/x-dtbncx+xml");
        mimeTypes.put("ppsx", "application/vnd.openxmlformats-officedocument.presentationml.slideshow");
        mimeTypes.put("sc", "application/vnd.ibm.secure-container");
        mimeTypes.put("gnumeric", "application/x-gnumeric");
        mimeTypes.put("paw", "application/vnd.pawaafile");
        mimeTypes.put("bmp", "image/bmp");
        mimeTypes.put("bmi", "application/vnd.bmi");
        mimeTypes.put("sxc", "application/vnd.sun.xml.calc");
        mimeTypes.put("gim", "application/vnd.groove-identity-message");
        mimeTypes.put("qps", "application/vnd.publishare-delta-tree");
        mimeTypes.put("scd", "application/x-msschedule");
        mimeTypes.put("mgz", "application/vnd.proteus.magazine");
        mimeTypes.put("vcf", "text/x-vcard");
        mimeTypes.put("flw", "application/vnd.kde.kivio");
        mimeTypes.put("xslt", "application/xslt+xml");
        mimeTypes.put("stc", "application/vnd.sun.xml.calc.template");
        mimeTypes.put("dir", "application/x-director");
        mimeTypes.put("dcr", "application/x-director");
        mimeTypes.put("dxr", "application/x-director");
        mimeTypes.put("cst", "application/x-director");
        mimeTypes.put("cct", "application/x-director");
        mimeTypes.put("cxt", "application/x-director");
        mimeTypes.put("w3d", "application/x-director");
        mimeTypes.put("fgd", "application/x-director");
        mimeTypes.put("swa", "application/x-director");
        mimeTypes.put("rif", "application/reginfo+xml");
        mimeTypes.put("atc", "application/vnd.acucorp");
        mimeTypes.put("acutc", "application/vnd.acucorp");
        mimeTypes.put("fti", "application/vnd.anser-web-funds-transfer-initiation");
        mimeTypes.put("dis", "application/vnd.mobius.dis");
        mimeTypes.put("zmm", "application/vnd.handheld-entertainment+xml");
        mimeTypes.put("mag", "application/vnd.ecowin.chart");
        mimeTypes.put("mlp", "application/vnd.dolby.mlp");
        mimeTypes.put("psb", "application/vnd.3gpp.pic-bw-small");
        mimeTypes.put("mdb", "application/x-msaccess");
        mimeTypes.put("aso", "application/vnd.accpac.simply.aso");
        mimeTypes.put("pwn", "application/vnd.3m.post-it-notes");
        mimeTypes.put("ots", "application/vnd.oasis.opendocument.spreadsheet-template");
        mimeTypes.put("mj2", "video/mj2");
        mimeTypes.put("mjp2", "video/mj2");
        mimeTypes.put("link66", "application/vnd.route66.link66+xml");
        mimeTypes.put("les", "application/vnd.hhe.lesson-player");
        mimeTypes.put("sti", "application/vnd.sun.xml.impress.template");
        mimeTypes.put("cdbcmsg", "application/vnd.contact.cmsg");
        mimeTypes.put("cdxml", "application/vnd.chemdraw+xml");
        mimeTypes.put("aac", "audio/x-aac");
        mimeTypes.put("ipfix", "application/ipfix");
        mimeTypes.put("mxu", "video/vnd.mpegurl");
        mimeTypes.put("m4u", "video/vnd.mpegurl");
        mimeTypes.put("cer", "application/pkix-cert");
        mimeTypes.put("mny", "application/x-msmoney");
        mimeTypes.put("tex", "application/x-tex");
        mimeTypes.put("ics", "text/calendar");
        mimeTypes.put("ifb", "text/calendar");
        mimeTypes.put("ei6", "application/vnd.pg.osasli");
        mimeTypes.put("spf", "application/vnd.yamaha.smaf-phrase");
        mimeTypes.put("lostxml", "application/lost+xml");
        mimeTypes.put("apk", "application/vnd.android.package-archive");
        mimeTypes.put("pub", "application/x-mspublisher");
        mimeTypes.put("gtw", "model/vnd.gtw");
        mimeTypes.put("ufd", "application/vnd.ufdl");
        mimeTypes.put("ufdl", "application/vnd.ufdl");
        mimeTypes.put("jnlp", "application/x-java-jnlp-file");
        mimeTypes.put("air", "application/vnd.adobe.air-application-installer-package+zip");
        mimeTypes.put("dcurl", "text/vnd.curl.dcurl");
        mimeTypes.put("mxl", "application/vnd.recordare.musicxml");
        mimeTypes.put("cpio", "application/x-cpio");
        mimeTypes.put("mpp", "application/vnd.ms-project");
        mimeTypes.put("mpt", "application/vnd.ms-project");
        mimeTypes.put("pdb", "application/vnd.palm");
        mimeTypes.put("pqa", "application/vnd.palm");
        mimeTypes.put("oprc", "application/vnd.palm");
        mimeTypes.put("mrc", "application/marc");
        mimeTypes.put("hqx", "application/mac-binhex40");
        mimeTypes.put("json", "application/json");
        mimeTypes.put("mvb", "application/x-msmediaview");
        mimeTypes.put("m13", "application/x-msmediaview");
        mimeTypes.put("m14", "application/x-msmediaview");
        mimeTypes.put("mpga", "audio/mpeg");
        mimeTypes.put("mp2", "audio/mpeg");
        mimeTypes.put("mp2a", "audio/mpeg");
        mimeTypes.put("mp3", "audio/mpeg");
        mimeTypes.put("m2a", "audio/mpeg");
        mimeTypes.put("m3a", "audio/mpeg");
        mimeTypes.put("cmc", "application/vnd.cosmocaller");
        mimeTypes.put("wmf", "application/x-msmetafile");
        mimeTypes.put("odft", "application/vnd.oasis.opendocument.formula-template");
        mimeTypes.put("dtb", "application/x-dtbook+xml");
        mimeTypes.put("xbap", "application/x-ms-xbap");
        mimeTypes.put("csp", "application/vnd.commonspace");
        mimeTypes.put("stk", "application/hyperstudio");
        mimeTypes.put("mpn", "application/vnd.mophun.application");
        mimeTypes.put("gqf", "application/vnd.grafeq");
        mimeTypes.put("gqs", "application/vnd.grafeq");
        mimeTypes.put("rl", "application/resource-lists+xml");
        mimeTypes.put("gtar", "application/x-gtar");
        mimeTypes.put("pls", "application/pls+xml");
        mimeTypes.put("tcl", "application/x-tcl");
        mimeTypes.put("der", "application/x-x509-ca-cert");
        mimeTypes.put("crt", "application/x-x509-ca-cert");
        mimeTypes.put("xo", "application/vnd.olpc-sugar");
        mimeTypes.put("xltm", "application/vnd.ms-excel.template.macroenabled.12");
        mimeTypes.put("cu", "application/cu-seeme");
        mimeTypes.put("kml", "application/vnd.google-earth.kml+xml");
        mimeTypes.put("sh", "application/x-sh");
        mimeTypes.put("xpw", "application/vnd.intercon.formnet");
        mimeTypes.put("xpx", "application/vnd.intercon.formnet");
        mimeTypes.put("wg", "application/vnd.pmi.widget");
        mimeTypes.put("seed", "application/vnd.fdsn.seed");
        mimeTypes.put("dataless", "application/vnd.fdsn.seed");
        mimeTypes.put("sdd", "application/vnd.stardivision.impress");
        mimeTypes.put("ttf", "application/x-font-ttf");
        mimeTypes.put("ttc", "application/x-font-ttf");
        mimeTypes.put("npx", "image/vnd.net-fpx");
        mimeTypes.put("aif", "audio/x-aiff");
        mimeTypes.put("aiff", "audio/x-aiff");
        mimeTypes.put("aifc", "audio/x-aiff");
        mimeTypes.put("xlsb", "application/vnd.ms-excel.sheet.binary.macroenabled.12");
        mimeTypes.put("wbmp", "image/vnd.wap.wbmp");
        mimeTypes.put("rtf", "application/rtf");
        mimeTypes.put("sus", "application/vnd.sus-calendar");
        mimeTypes.put("susp", "application/vnd.sus-calendar");
        mimeTypes.put("prf", "application/pics-rules");
        mimeTypes.put("tar", "application/x-tar");
        mimeTypes.put("pml", "application/vnd.ctc-posml");
        mimeTypes.put("ims", "application/vnd.ms-ims");
        mimeTypes.put("imp", "application/vnd.accpac.simply.imp");
        mimeTypes.put("xul", "application/vnd.mozilla.xul+xml");
        mimeTypes.put("acc", "application/vnd.americandynamics.acc");
        mimeTypes.put("mfm", "application/vnd.mfmp");
        mimeTypes.put("dotm", "application/vnd.ms-word.template.macroenabled.12");
        mimeTypes.put("ptid", "application/vnd.pvi.ptid1");
        mimeTypes.put("pyv", "video/vnd.ms-playready.media.pyv");
        mimeTypes.put("ssf", "application/vnd.epson.ssf");
        mimeTypes.put("sxd", "application/vnd.sun.xml.draw");
        mimeTypes.put("xap", "application/x-silverlight-app");
        mimeTypes.put("fst", "image/vnd.fst");
        mimeTypes.put("rdf", "application/rdf+xml");
        mimeTypes.put("gv", "text/vnd.graphviz");
        mimeTypes.put("lrm", "application/vnd.ms-lrm");
        mimeTypes.put("box", "application/vnd.previewsystems.box");
        mimeTypes.put("mseq", "application/vnd.mseq");
        mimeTypes.put("xwd", "image/x-xwindowdump");
        mimeTypes.put("mscml", "application/mediaservercontrol+xml");
        mimeTypes.put("cmp", "application/vnd.yellowriver-custom-menu");
        mimeTypes.put("wad", "application/x-doom");
        mimeTypes.put("svd", "application/vnd.svd");
        mimeTypes.put("pki", "application/pkixcmp");
        mimeTypes.put("ai", "application/postscript");
        mimeTypes.put("eps", "application/postscript");
        mimeTypes.put("ps", "application/postscript");
        mimeTypes.put("msl", "application/vnd.mobius.msl");
        mimeTypes.put("sv4cpio", "application/x-sv4cpio");
        mimeTypes.put("java", "text/x-java-source");
        mimeTypes.put("mpc", "application/vnd.mophun.certificate");
        mimeTypes.put("daf", "application/vnd.mobius.daf");
        mimeTypes.put("qfx", "application/vnd.intu.qfx");
        mimeTypes.put("mxf", "application/mxf");
        mimeTypes.put("mif", "application/vnd.mif");
        mimeTypes.put("txd", "application/vnd.genomatix.tuxedo");
        mimeTypes.put("pkipath", "application/pkix-pkipath");
        mimeTypes.put("sse", "application/vnd.kodak-descriptor");
        mimeTypes.put("kon", "application/vnd.kde.kontour");
        mimeTypes.put("dfac", "application/vnd.dreamfactory");
        mimeTypes.put("gram", "application/srgs");
        mimeTypes.put("hps", "application/vnd.hp-hps");
        mimeTypes.put("cab", "application/vnd.ms-cab-compressed");
        mimeTypes.put("m3u", "audio/x-mpegurl");
        mimeTypes.put("odp", "application/vnd.oasis.opendocument.presentation");
        mimeTypes.put("ggb", "application/vnd.geogebra.file");
        mimeTypes.put("xyz", "chemical/x-xyz");
        mimeTypes.put("clkw", "application/vnd.crick.clicker.wordbank");
        mimeTypes.put("mqy", "application/vnd.mobius.mqy");
        mimeTypes.put("ico", "image/x-icon");
        mimeTypes.put("png", "image/png");
        mimeTypes.put("wmlc", "application/vnd.wap.wmlc");
        mimeTypes.put("kne", "application/vnd.kinar");
        mimeTypes.put("knp", "application/vnd.kinar");
        mimeTypes.put("kpr", "application/vnd.kde.kpresenter");
        mimeTypes.put("kpt", "application/vnd.kde.kpresenter");
        mimeTypes.put("sbml", "application/sbml+xml");
        mimeTypes.put("fpx", "image/vnd.fpx");
        mimeTypes.put("bz", "application/x-bzip");
        mimeTypes.put("flx", "text/vnd.fmi.flexstor");
        mimeTypes.put("application", "application/x-ms-application");
        mimeTypes.put("wmlsc", "application/vnd.wap.wmlscriptc");
        mimeTypes.put("lbd", "application/vnd.llamagraphics.life-balance.desktop");
        mimeTypes.put("sxw", "application/vnd.sun.xml.writer");
        mimeTypes.put("jam", "application/vnd.jam");
        mimeTypes.put("musicxml", "application/vnd.recordare.musicxml+xml");
        mimeTypes.put("see", "application/vnd.seemail");
        mimeTypes.put("irp", "application/vnd.irepository.package+xml");
        mimeTypes.put("tiff", "image/tiff");
        mimeTypes.put("tif", "image/tiff");
        mimeTypes.put("aam", "application/x-authorware-map");
        mimeTypes.put("chat", "application/x-chat");
        mimeTypes.put("mpkg", "application/vnd.apple.installer+xml");
        mimeTypes.put("otc", "application/vnd.oasis.opendocument.chart-template");
        mimeTypes.put("msh", "model/mesh");
        mimeTypes.put("mesh", "model/mesh");
        mimeTypes.put("silo", "model/mesh");
        mimeTypes.put("t", "text/troff");
        mimeTypes.put("tr", "text/troff");
        mimeTypes.put("roff", "text/troff");
        mimeTypes.put("man", "text/troff");
        mimeTypes.put("me", "text/troff");
        mimeTypes.put("ms", "text/troff");
        mimeTypes.put("dpg", "application/vnd.dpgraph");
        mimeTypes.put("wri", "application/x-mswrite");
        mimeTypes.put("dts", "audio/vnd.dts");
        mimeTypes.put("xpi", "application/x-xpinstall");
        mimeTypes.put("ram", "audio/x-pn-realaudio");
        mimeTypes.put("ra", "audio/x-pn-realaudio");
        mimeTypes.put("sdkm", "application/vnd.solent.sdkm+xml");
        mimeTypes.put("sdkd", "application/vnd.solent.sdkm+xml");
        mimeTypes.put("dtshd", "audio/vnd.dts.hd");
        mimeTypes.put("btif", "image/prs.btif");
        mimeTypes.put("scs", "application/scvp-cv-response");
        mimeTypes.put("car", "application/vnd.curl.car");
        mimeTypes.put("otf", "application/x-font-otf");
        mimeTypes.put("clkx", "application/vnd.crick.clicker");
        mimeTypes.put("xbd", "application/vnd.fujixerox.docuworks.binder");
        mimeTypes.put("ppm", "image/x-portable-pixmap");
        mimeTypes.put("wav", "audio/x-wav");
        mimeTypes.put("ssml", "application/ssml+xml");
        mimeTypes.put("p7b", "application/x-pkcs7-certificates");
        mimeTypes.put("spc", "application/x-pkcs7-certificates");
        mimeTypes.put("kia", "application/vnd.kidspiration");
        mimeTypes.put("rss", "application/rss+xml");
        mimeTypes.put("setreg", "application/set-registration-initiation");
        mimeTypes.put("qbo", "application/vnd.intu.qbo");
        mimeTypes.put("ras", "image/x-cmu-raster");
        mimeTypes.put("rar", "application/x-rar-compressed");
        mimeTypes.put("ogx", "application/ogg");
        mimeTypes.put("class", "application/java-vm");
        mimeTypes.put("smf", "application/vnd.stardivision.math");
        mimeTypes.put("atom", "application/atom+xml");
        mimeTypes.put("sit", "application/x-stuffit");
        mimeTypes.put("ez3", "application/vnd.ezpix-package");
        mimeTypes.put("mcurl", "text/vnd.curl.mcurl");
        mimeTypes.put("wmls", "text/vnd.wap.wmlscript");
        mimeTypes.put("srx", "application/sparql-results+xml");
        mimeTypes.put("wps", "application/vnd.ms-works");
        mimeTypes.put("wks", "application/vnd.ms-works");
        mimeTypes.put("wcm", "application/vnd.ms-works");
        mimeTypes.put("wdb", "application/vnd.ms-works");
        mimeTypes.put("vcs", "text/x-vcalendar");
        mimeTypes.put("ecma", "application/ecmascript");
        mimeTypes.put("curl", "text/vnd.curl");
        mimeTypes.put("std", "application/vnd.sun.xml.draw.template");
        mimeTypes.put("eot", "application/vnd.ms-fontobject");
        mimeTypes.put("fsc", "application/vnd.fsc.weblaunch");
        mimeTypes.put("tfm", "application/x-tex-tfm");
        mimeTypes.put("dra", "audio/vnd.dra");
        mimeTypes.put("mwf", "application/vnd.mfer");
        mimeTypes.put("hpid", "application/vnd.hp-hpid");
        mimeTypes.put("nml", "application/vnd.enliven");
        mimeTypes.put("hvp", "application/vnd.yamaha.hv-voice");
        mimeTypes.put("s", "text/x-asm");
        mimeTypes.put("asm", "text/x-asm");
        mimeTypes.put("mcd", "application/vnd.mcd");
        mimeTypes.put("mts", "model/vnd.mts");
        mimeTypes.put("igl", "application/vnd.igloader");
        mimeTypes.put("tao", "application/vnd.tao.intent-module-archive");
        mimeTypes.put("sgml", "text/sgml");
        mimeTypes.put("sgm", "text/sgml");
        mimeTypes.put("rmp", "audio/x-pn-realaudio-plugin");
        mimeTypes.put("xenc", "application/xenc+xml");
        mimeTypes.put("wpl", "application/vnd.ms-wpl");
        mimeTypes.put("dxf", "image/vnd.dxf");
        mimeTypes.put("pgm", "image/x-portable-graymap");
        mimeTypes.put("spot", "text/vnd.in3d.spot");
        mimeTypes.put("odt", "application/vnd.oasis.opendocument.text");
        mimeTypes.put("azs", "application/vnd.airzip.filesecure.azs");
        mimeTypes.put("es3", "application/vnd.eszigno3+xml");
        mimeTypes.put("et3", "application/vnd.eszigno3+xml");
        mimeTypes.put("dd2", "application/vnd.oma.dd2+xml");
        mimeTypes.put("semf", "application/vnd.semf");
        mimeTypes.put("semd", "application/vnd.semd");
        mimeTypes.put("pnm", "image/x-portable-anymap");
        mimeTypes.put("sema", "application/vnd.sema");
        mimeTypes.put("wma", "audio/x-ms-wma");
        mimeTypes.put("cww", "application/prs.cww");
        mimeTypes.put("scm", "application/vnd.lotus-screencam");
        mimeTypes.put("azf", "application/vnd.airzip.filesecure.azf");
        mimeTypes.put("oda", "application/oda");
        mimeTypes.put("dwg", "image/vnd.dwg");
        mimeTypes.put("h264", "video/h264");
        mimeTypes.put("hpgl", "application/vnd.hp-hpgl");
        mimeTypes.put("xpr", "application/vnd.is-xpr");
        mimeTypes.put("h263", "video/h263");
        mimeTypes.put("zip", "application/zip");
        mimeTypes.put("h261", "video/h261");
        mimeTypes.put("oti", "application/vnd.oasis.opendocument.image-template");
        mimeTypes.put("uoml", "application/vnd.uoml+xml");
        mimeTypes.put("xspf", "application/xspf+xml");
        mimeTypes.put("ppam", "application/vnd.ms-powerpoint.addin.macroenabled.12");
        mimeTypes.put("dtd", "application/xml-dtd");
        mimeTypes.put("gex", "application/vnd.geometry-explorer");
        mimeTypes.put("gre", "application/vnd.geometry-explorer");
        mimeTypes.put("dssc", "application/dssc+der");
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

            /**
             * File format used by digital cameras to store images. Exif Format can be read by any application supporting JPEG. Exif Spec can be found at:
             * http://www.pima.net/standards/it10/PIMA15740/Exif_2-1.PDF
             */
            if ((c4 == 0xE1) && (c7 == 'E' && c8 == 'x' && c9 == 'i' && c10 == 'f' && c11 == 0))
                return "image/jpeg";

            if (c4 == 0xEE)
                return "image/jpg";
        }

        /**
         * According to http://www.opendesign.com/files/guestdownloads/OpenDesign_Specification_for_.dwg_files.pdf
         * first 6 bytes are of type "AC1018" (for example) and the next 5 bytes are 0x00.
         */
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

            return mimeTypes.get(extension);
        }

        return null;
    }
}