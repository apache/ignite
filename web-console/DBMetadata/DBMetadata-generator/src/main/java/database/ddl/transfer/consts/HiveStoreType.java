package database.ddl.transfer.consts;
/**
 *@ClassName StoreType
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-03 14:50
 *@Version
 **/
public class HiveStoreType {
	public static final String TEXTFILE = "TEXTFILE";
    public static final String SEQUENCEFILE = "SEQUENCEFILE";
    public static final String RCFILE = "RCFILE";
    public static final String ORCFILE = "ORCFILE";
    public static final String PARQUET = "PARQUET";

    public static final String FEATURE_TEXTFILE = "org.apache.hadoop.mapred.TextInputFormat";
    public static final String FEATURE_SEQUENCEFILE = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    public static final String FEATURE_RCFILE = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    public static final String FEATURE_ORCFILE = "org.apache.hadoop.hive.ql.io.orc";
    public static final String FEATURE_PARQUET = "org.apache.hadoop.hive.ql.io.parquet";
}
