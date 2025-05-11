package database.ddl.transfer.consts;

/**
 * @ClassName DataBaseTypeProperties
 * @Description TODO
 * @Author luoyuntian
 * @Date 2019-12-31 09:39
 * @Version
 **/
public class DataBaseTypeProperties {
	// 配置内容为需要添加长度或精度的类型
	public static final String MYSQL_TYPE_SCALA = "int,float,double,decimal,char,varchar,binary,varbinary,date,datetime,timestamp";
	public static final String POSTGRE_TYPE_SCALA = "bit,bit varying,character,char,character varying,varchar,interval,numeric,decimal,time,timestamp";
	public static final String ORACLE_TYPE_SCALA = "decimal,numeric,dec,number,char,varchar2";

}
