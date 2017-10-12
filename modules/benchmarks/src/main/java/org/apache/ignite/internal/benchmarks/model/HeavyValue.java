package org.apache.ignite.internal.benchmarks.model;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class HeavyValue {
    /**
     *
     */
    static public HeavyValue generate() {
        Integer i = ThreadLocalRandom.current().nextInt();
        Double d = ThreadLocalRandom.current().nextDouble();
        String s = i.toString();

        return new HeavyValue(
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            d,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            d,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            d,
            s,
            d,
            d,
            s,
            s,
            s,
            d,
            s
        );
    }

    String ACCOUNTCODE;
    String ASSETTYPE;
    String ASSETUNIT;
    String ATLASFOLDERID;
    String ATLASINSTRUMENTSTRUCTUREPATH;
    String BOOKSOURCESYSTEM;
    String BOOKSOURCESYSTEMCODE;
    String BUSINESSDATE;
    String CUSIP;
    String DATASETFILTER;
    String DATASETLABEL;
    Double EODTOTALVALUE;
    String ESMP;
    String FOAGGRCODE;
    String HOSTPRODID;
    String INSTRUMENTEXPIRYDATE;
    String INSTRUMENTMATURITYDATE;
    String INSTRUMENTTYPE;
    String ISIN;
    String PROXYINSTRUMENTID;
    String PROXYINSTRUMENTIDTYPE;
    String PROXYINSTRUMENTTYPE;
    Double QUANTITY;
    String REGION;
    String RIC;
    String RISKFACTORNAME;
    String RISKPARENTINSTRUMENTID;
    String RISKPARENTINSTRUMENTIDTYPE;
    String RISKSOURCESYSTEM;
    String RISKSUBJECTCHORUSBOOKID;
    String RISKSUBJECTID;
    String RISKSUBJECTINSTRUMENTCOUNTERPARTYID;
    String RISKSUBJECTINSTRUMENTID;
    String RISKSUBJECTINSTRUMENTIDTYPE;
    String RISKSUBJECTSOURCE;
    String RISKSUBJECTTYPE;
    String SENSITIVITYTYPE;
    String SERIESDATE;
    String SERIESDAY;
    String SNAPVERSION;
    Double STRIKEVALUE;
    String SYS_AUDIT_TRACE;
    Double THEOPRICE;
    Double TOTALVALUE;
    String UNDERLYINGSECURITYID;
    String UNDERLYINGSECURITYIDTYPE;
    String VALUATIONSOURCECONTEXTLABELNAME;
    Double VALUE;
    String VARTYPE;

    public HeavyValue(String ACCOUNTCODE, String ASSETTYPE, String ASSETUNIT, String ATLASFOLDERID,
        String ATLASINSTRUMENTSTRUCTUREPATH, String BOOKSOURCESYSTEM, String BOOKSOURCESYSTEMCODE,
        String BUSINESSDATE, String CUSIP, String DATASETFILTER, String DATASETLABEL, Double EODTOTALVALUE,
        String ESMP, String FOAGGRCODE, String HOSTPRODID, String INSTRUMENTEXPIRYDATE,
        String INSTRUMENTMATURITYDATE, String INSTRUMENTTYPE, String ISIN, String PROXYINSTRUMENTID,
        String PROXYINSTRUMENTIDTYPE, String PROXYINSTRUMENTTYPE, Double QUANTITY, String REGION, String RIC,
        String RISKFACTORNAME, String RISKPARENTINSTRUMENTID, String RISKPARENTINSTRUMENTIDTYPE,
        String RISKSOURCESYSTEM, String RISKSUBJECTCHORUSBOOKID, String RISKSUBJECTID,
        String RISKSUBJECTINSTRUMENTCOUNTERPARTYID, String RISKSUBJECTINSTRUMENTID,
        String RISKSUBJECTINSTRUMENTIDTYPE, String RISKSUBJECTSOURCE, String RISKSUBJECTTYPE,
        String SENSITIVITYTYPE, String SERIESDATE, String SERIESDAY, String SNAPVERSION, Double STRIKEVALUE,
        String SYS_AUDIT_TRACE, Double THEOPRICE, Double TOTALVALUE, String UNDERLYINGSECURITYID,
        String UNDERLYINGSECURITYIDTYPE, String VALUATIONSOURCECONTEXTLABELNAME, Double VALUE, String VARTYPE) {
        this.ACCOUNTCODE = ACCOUNTCODE;
        this.ASSETTYPE = ASSETTYPE;
        this.ASSETUNIT = ASSETUNIT;
        this.ATLASFOLDERID = ATLASFOLDERID;
        this.ATLASINSTRUMENTSTRUCTUREPATH = ATLASINSTRUMENTSTRUCTUREPATH;
        this.BOOKSOURCESYSTEM = BOOKSOURCESYSTEM;
        this.BOOKSOURCESYSTEMCODE = BOOKSOURCESYSTEMCODE;
        this.BUSINESSDATE = BUSINESSDATE;
        this.CUSIP = CUSIP;
        this.DATASETFILTER = DATASETFILTER;
        this.DATASETLABEL = DATASETLABEL;
        this.EODTOTALVALUE = EODTOTALVALUE;
        this.ESMP = ESMP;
        this.FOAGGRCODE = FOAGGRCODE;
        this.HOSTPRODID = HOSTPRODID;
        this.INSTRUMENTEXPIRYDATE = INSTRUMENTEXPIRYDATE;
        this.INSTRUMENTMATURITYDATE = INSTRUMENTMATURITYDATE;
        this.INSTRUMENTTYPE = INSTRUMENTTYPE;
        this.ISIN = ISIN;
        this.PROXYINSTRUMENTID = PROXYINSTRUMENTID;
        this.PROXYINSTRUMENTIDTYPE = PROXYINSTRUMENTIDTYPE;
        this.PROXYINSTRUMENTTYPE = PROXYINSTRUMENTTYPE;
        this.QUANTITY = QUANTITY;
        this.REGION = REGION;
        this.RIC = RIC;
        this.RISKFACTORNAME = RISKFACTORNAME;
        this.RISKPARENTINSTRUMENTID = RISKPARENTINSTRUMENTID;
        this.RISKPARENTINSTRUMENTIDTYPE = RISKPARENTINSTRUMENTIDTYPE;
        this.RISKSOURCESYSTEM = RISKSOURCESYSTEM;
        this.RISKSUBJECTCHORUSBOOKID = RISKSUBJECTCHORUSBOOKID;
        this.RISKSUBJECTID = RISKSUBJECTID;
        this.RISKSUBJECTINSTRUMENTCOUNTERPARTYID = RISKSUBJECTINSTRUMENTCOUNTERPARTYID;
        this.RISKSUBJECTINSTRUMENTID = RISKSUBJECTINSTRUMENTID;
        this.RISKSUBJECTINSTRUMENTIDTYPE = RISKSUBJECTINSTRUMENTIDTYPE;
        this.RISKSUBJECTSOURCE = RISKSUBJECTSOURCE;
        this.RISKSUBJECTTYPE = RISKSUBJECTTYPE;
        this.SENSITIVITYTYPE = SENSITIVITYTYPE;
        this.SERIESDATE = SERIESDATE;
        this.SERIESDAY = SERIESDAY;
        this.SNAPVERSION = SNAPVERSION;
        this.STRIKEVALUE = STRIKEVALUE;
        this.SYS_AUDIT_TRACE = SYS_AUDIT_TRACE;
        this.THEOPRICE = THEOPRICE;
        this.TOTALVALUE = TOTALVALUE;
        this.UNDERLYINGSECURITYID = UNDERLYINGSECURITYID;
        this.UNDERLYINGSECURITYIDTYPE = UNDERLYINGSECURITYIDTYPE;
        this.VALUATIONSOURCECONTEXTLABELNAME = VALUATIONSOURCECONTEXTLABELNAME;
        this.VALUE = VALUE;
        this.VARTYPE = VARTYPE;
    }

    public String getACCOUNTCODE() {
        return ACCOUNTCODE;
    }

    public void setACCOUNTCODE(String ACCOUNTCODE) {
        this.ACCOUNTCODE = ACCOUNTCODE;
    }

    public String getASSETTYPE() {
        return ASSETTYPE;
    }

    public void setASSETTYPE(String ASSETTYPE) {
        this.ASSETTYPE = ASSETTYPE;
    }

    public String getASSETUNIT() {
        return ASSETUNIT;
    }

    public void setASSETUNIT(String ASSETUNIT) {
        this.ASSETUNIT = ASSETUNIT;
    }

    public String getATLASFOLDERID() {
        return ATLASFOLDERID;
    }

    public void setATLASFOLDERID(String ATLASFOLDERID) {
        this.ATLASFOLDERID = ATLASFOLDERID;
    }

    public String getATLASINSTRUMENTSTRUCTUREPATH() {
        return ATLASINSTRUMENTSTRUCTUREPATH;
    }

    public void setATLASINSTRUMENTSTRUCTUREPATH(String ATLASINSTRUMENTSTRUCTUREPATH) {
        this.ATLASINSTRUMENTSTRUCTUREPATH = ATLASINSTRUMENTSTRUCTUREPATH;
    }

    public String getBOOKSOURCESYSTEM() {
        return BOOKSOURCESYSTEM;
    }

    public void setBOOKSOURCESYSTEM(String BOOKSOURCESYSTEM) {
        this.BOOKSOURCESYSTEM = BOOKSOURCESYSTEM;
    }

    public String getBOOKSOURCESYSTEMCODE() {
        return BOOKSOURCESYSTEMCODE;
    }

    public void setBOOKSOURCESYSTEMCODE(String BOOKSOURCESYSTEMCODE) {
        this.BOOKSOURCESYSTEMCODE = BOOKSOURCESYSTEMCODE;
    }

    public String getBUSINESSDATE() {
        return BUSINESSDATE;
    }

    public void setBUSINESSDATE(String BUSINESSDATE) {
        this.BUSINESSDATE = BUSINESSDATE;
    }

    public String getCUSIP() {
        return CUSIP;
    }

    public void setCUSIP(String CUSIP) {
        this.CUSIP = CUSIP;
    }

    public String getDATASETFILTER() {
        return DATASETFILTER;
    }

    public void setDATASETFILTER(String DATASETFILTER) {
        this.DATASETFILTER = DATASETFILTER;
    }

    public String getDATASETLABEL() {
        return DATASETLABEL;
    }

    public void setDATASETLABEL(String DATASETLABEL) {
        this.DATASETLABEL = DATASETLABEL;
    }

    public Double getEODTOTALVALUE() {
        return EODTOTALVALUE;
    }

    public void setEODTOTALVALUE(Double EODTOTALVALUE) {
        this.EODTOTALVALUE = EODTOTALVALUE;
    }

    public String getESMP() {
        return ESMP;
    }

    public void setESMP(String ESMP) {
        this.ESMP = ESMP;
    }

    public String getFOAGGRCODE() {
        return FOAGGRCODE;
    }

    public void setFOAGGRCODE(String FOAGGRCODE) {
        this.FOAGGRCODE = FOAGGRCODE;
    }

    public String getHOSTPRODID() {
        return HOSTPRODID;
    }

    public void setHOSTPRODID(String HOSTPRODID) {
        this.HOSTPRODID = HOSTPRODID;
    }

    public String getINSTRUMENTEXPIRYDATE() {
        return INSTRUMENTEXPIRYDATE;
    }

    public void setINSTRUMENTEXPIRYDATE(String INSTRUMENTEXPIRYDATE) {
        this.INSTRUMENTEXPIRYDATE = INSTRUMENTEXPIRYDATE;
    }

    public String getINSTRUMENTMATURITYDATE() {
        return INSTRUMENTMATURITYDATE;
    }

    public void setINSTRUMENTMATURITYDATE(String INSTRUMENTMATURITYDATE) {
        this.INSTRUMENTMATURITYDATE = INSTRUMENTMATURITYDATE;
    }

    public String getINSTRUMENTTYPE() {
        return INSTRUMENTTYPE;
    }

    public void setINSTRUMENTTYPE(String INSTRUMENTTYPE) {
        this.INSTRUMENTTYPE = INSTRUMENTTYPE;
    }

    public String getISIN() {
        return ISIN;
    }

    public void setISIN(String ISIN) {
        this.ISIN = ISIN;
    }

    public String getPROXYINSTRUMENTID() {
        return PROXYINSTRUMENTID;
    }

    public void setPROXYINSTRUMENTID(String PROXYINSTRUMENTID) {
        this.PROXYINSTRUMENTID = PROXYINSTRUMENTID;
    }

    public String getPROXYINSTRUMENTIDTYPE() {
        return PROXYINSTRUMENTIDTYPE;
    }

    public void setPROXYINSTRUMENTIDTYPE(String PROXYINSTRUMENTIDTYPE) {
        this.PROXYINSTRUMENTIDTYPE = PROXYINSTRUMENTIDTYPE;
    }

    public String getPROXYINSTRUMENTTYPE() {
        return PROXYINSTRUMENTTYPE;
    }

    public void setPROXYINSTRUMENTTYPE(String PROXYINSTRUMENTTYPE) {
        this.PROXYINSTRUMENTTYPE = PROXYINSTRUMENTTYPE;
    }

    public Double getQUANTITY() {
        return QUANTITY;
    }

    public void setQUANTITY(Double QUANTITY) {
        this.QUANTITY = QUANTITY;
    }

    public String getREGION() {
        return REGION;
    }

    public void setREGION(String REGION) {
        this.REGION = REGION;
    }

    public String getRIC() {
        return RIC;
    }

    public void setRIC(String RIC) {
        this.RIC = RIC;
    }

    public String getRISKFACTORNAME() {
        return RISKFACTORNAME;
    }

    public void setRISKFACTORNAME(String RISKFACTORNAME) {
        this.RISKFACTORNAME = RISKFACTORNAME;
    }

    public String getRISKPARENTINSTRUMENTID() {
        return RISKPARENTINSTRUMENTID;
    }

    public void setRISKPARENTINSTRUMENTID(String RISKPARENTINSTRUMENTID) {
        this.RISKPARENTINSTRUMENTID = RISKPARENTINSTRUMENTID;
    }

    public String getRISKPARENTINSTRUMENTIDTYPE() {
        return RISKPARENTINSTRUMENTIDTYPE;
    }

    public void setRISKPARENTINSTRUMENTIDTYPE(String RISKPARENTINSTRUMENTIDTYPE) {
        this.RISKPARENTINSTRUMENTIDTYPE = RISKPARENTINSTRUMENTIDTYPE;
    }

    public String getRISKSOURCESYSTEM() {
        return RISKSOURCESYSTEM;
    }

    public void setRISKSOURCESYSTEM(String RISKSOURCESYSTEM) {
        this.RISKSOURCESYSTEM = RISKSOURCESYSTEM;
    }

    public String getRISKSUBJECTCHORUSBOOKID() {
        return RISKSUBJECTCHORUSBOOKID;
    }

    public void setRISKSUBJECTCHORUSBOOKID(String RISKSUBJECTCHORUSBOOKID) {
        this.RISKSUBJECTCHORUSBOOKID = RISKSUBJECTCHORUSBOOKID;
    }

    public String getRISKSUBJECTID() {
        return RISKSUBJECTID;
    }

    public void setRISKSUBJECTID(String RISKSUBJECTID) {
        this.RISKSUBJECTID = RISKSUBJECTID;
    }

    public String getRISKSUBJECTINSTRUMENTCOUNTERPARTYID() {
        return RISKSUBJECTINSTRUMENTCOUNTERPARTYID;
    }

    public void setRISKSUBJECTINSTRUMENTCOUNTERPARTYID(String RISKSUBJECTINSTRUMENTCOUNTERPARTYID) {
        this.RISKSUBJECTINSTRUMENTCOUNTERPARTYID = RISKSUBJECTINSTRUMENTCOUNTERPARTYID;
    }

    public String getRISKSUBJECTINSTRUMENTID() {
        return RISKSUBJECTINSTRUMENTID;
    }

    public void setRISKSUBJECTINSTRUMENTID(String RISKSUBJECTINSTRUMENTID) {
        this.RISKSUBJECTINSTRUMENTID = RISKSUBJECTINSTRUMENTID;
    }

    public String getRISKSUBJECTINSTRUMENTIDTYPE() {
        return RISKSUBJECTINSTRUMENTIDTYPE;
    }

    public void setRISKSUBJECTINSTRUMENTIDTYPE(String RISKSUBJECTINSTRUMENTIDTYPE) {
        this.RISKSUBJECTINSTRUMENTIDTYPE = RISKSUBJECTINSTRUMENTIDTYPE;
    }

    public String getRISKSUBJECTSOURCE() {
        return RISKSUBJECTSOURCE;
    }

    public void setRISKSUBJECTSOURCE(String RISKSUBJECTSOURCE) {
        this.RISKSUBJECTSOURCE = RISKSUBJECTSOURCE;
    }

    public String getRISKSUBJECTTYPE() {
        return RISKSUBJECTTYPE;
    }

    public void setRISKSUBJECTTYPE(String RISKSUBJECTTYPE) {
        this.RISKSUBJECTTYPE = RISKSUBJECTTYPE;
    }

    public String getSENSITIVITYTYPE() {
        return SENSITIVITYTYPE;
    }

    public void setSENSITIVITYTYPE(String SENSITIVITYTYPE) {
        this.SENSITIVITYTYPE = SENSITIVITYTYPE;
    }

    public String getSERIESDATE() {
        return SERIESDATE;
    }

    public void setSERIESDATE(String SERIESDATE) {
        this.SERIESDATE = SERIESDATE;
    }

    public String getSERIESDAY() {
        return SERIESDAY;
    }

    public void setSERIESDAY(String SERIESDAY) {
        this.SERIESDAY = SERIESDAY;
    }

    public String getSNAPVERSION() {
        return SNAPVERSION;
    }

    public void setSNAPVERSION(String SNAPVERSION) {
        this.SNAPVERSION = SNAPVERSION;
    }

    public Double getSTRIKEVALUE() {
        return STRIKEVALUE;
    }

    public void setSTRIKEVALUE(Double STRIKEVALUE) {
        this.STRIKEVALUE = STRIKEVALUE;
    }

    public String getSYS_AUDIT_TRACE() {
        return SYS_AUDIT_TRACE;
    }

    public void setSYS_AUDIT_TRACE(String SYS_AUDIT_TRACE) {
        this.SYS_AUDIT_TRACE = SYS_AUDIT_TRACE;
    }

    public Double getTHEOPRICE() {
        return THEOPRICE;
    }

    public void setTHEOPRICE(Double THEOPRICE) {
        this.THEOPRICE = THEOPRICE;
    }

    public Double getTOTALVALUE() {
        return TOTALVALUE;
    }

    public void setTOTALVALUE(Double TOTALVALUE) {
        this.TOTALVALUE = TOTALVALUE;
    }

    public String getUNDERLYINGSECURITYID() {
        return UNDERLYINGSECURITYID;
    }

    public void setUNDERLYINGSECURITYID(String UNDERLYINGSECURITYID) {
        this.UNDERLYINGSECURITYID = UNDERLYINGSECURITYID;
    }

    public String getUNDERLYINGSECURITYIDTYPE() {
        return UNDERLYINGSECURITYIDTYPE;
    }

    public void setUNDERLYINGSECURITYIDTYPE(String UNDERLYINGSECURITYIDTYPE) {
        this.UNDERLYINGSECURITYIDTYPE = UNDERLYINGSECURITYIDTYPE;
    }

    public String getVALUATIONSOURCECONTEXTLABELNAME() {
        return VALUATIONSOURCECONTEXTLABELNAME;
    }

    public void setVALUATIONSOURCECONTEXTLABELNAME(String VALUATIONSOURCECONTEXTLABELNAME) {
        this.VALUATIONSOURCECONTEXTLABELNAME = VALUATIONSOURCECONTEXTLABELNAME;
    }

    public Double getVALUE() {
        return VALUE;
    }

    public void setVALUE(Double VALUE) {
        this.VALUE = VALUE;
    }

    public String getVARTYPE() {
        return VARTYPE;
    }

    public void setVARTYPE(String VARTYPE) {
        this.VARTYPE = VARTYPE;
    }
}