package database.ddl.transfer.bean;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.utils.StringUtil;

/**
 * 列定义
 *
 * @author gs
 */
public class Column {

	/**
	 * 源数据库类型
	 */
	private DataBaseType dataBaseType;

	/**
	 * 表名
	 */
	private String tableName;

	/**
	 * 列名
	 */
	private String columnName;

	/**
	 * 列数据类型 例如：bigint(20)
	 */
	private String columnType;

	/**
	 * 列顺序
	 */
	private int columnOrder;

	/**
	 * （字符）最大长度
	 */
	private Integer strLength;

	/**
	 * 精度
	 */
	private Integer precision;

	/**
	 * 精度(小数点后)
	 */
	private Integer scale;

	/**
	 * 是否允许为空
	 */
	private boolean nullAble;

	/**
	 * 默认值定义
	 */
	private String defaultDefine;

	/**
	 * 字段描述
	 */
	private String columnComment;

	/**
	 * 默认值更新策略
	 */
	private String extra;

	/**
	 * 字段约束策略   mysql:PRI主键约束/UNI唯一约束/MUL可以重复；   postgresql:c = 检查约束, f = 外键约束, p = 主键约束,
	 * u = 唯一约束, t = 约束触发器, x = 排除约束；  oracle:C = 检查约束，P = 主键约束，R = 外键约束，U = 唯一约束，
	 * O = Read Only on a view，V = Check Option on a view
	 */
	private String columnKey;

	/**
	 * 数据类型 例如：bigint
	 */
	private String dataType;
	
	/**
	 * 最终转换数据类型
	 */
	private String finalConvertDataType;

	public DataBaseType getDataBaseType() {
		return dataBaseType;
	}

	public void setDataBaseType(DataBaseType dataBaseType) {
		this.dataBaseType = dataBaseType;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public int getColumnOrder() {
		return columnOrder;
	}

	public void setColumnOrder(int columnOrder) {
		this.columnOrder = columnOrder;
	}

	public Integer getStrLength() {
		return strLength;
	}

	public void setStrLength(Integer strLength) {
		this.strLength = strLength;
	}

	public Integer getPrecision() {
		return precision;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public boolean isNullAble() {
		return nullAble;
	}

	public void setNullAble(boolean nullAble) {
		this.nullAble = nullAble;
	}

	public String getDefaultDefine() {
		if (this.defaultDefine != null && (this.isStringType() || this.isTextType()) && DataBaseType.MYSQL == dataBaseType) {
			return "'" + this.defaultDefine + "'";
		}
		return this.defaultDefine;
	}

	public String getColumnComment() {
		return columnComment;
	}

	public void setColumnComment(String columnComment) {
		this.columnComment = columnComment;
	}

	public String getExtra() {
		return extra;
	}

	public void setExtra(String extra) {
		this.extra = extra;
	}

	public String getColumnKey() {
		return columnKey;
	}

	public void setColumnKey(String columnKey) {
		this.columnKey = columnKey;
	}

	public Integer getScale() {
		return scale;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	public String getFinalConvertDataType() {
		return finalConvertDataType;
	}

	public void setFinalConvertDataType(String finalConvertDataType) {
		this.finalConvertDataType = finalConvertDataType;
	}

	/**
	 * 判断字段定义是否为字符串类型
	 * 
	 * @return 若字段定义为字符串类型，则返回true
	 */
	public boolean isStringType() {
		if (StringUtil.isBlank(this.dataType)) {
			return false;
		}

		return this.dataType.toLowerCase().indexOf("char") != -1;
	}

	public void setDefaultDefine(String defaultDefine) {
		this.defaultDefine = defaultDefine;
	}

	/**
	 * 字段数据类型是否有长度属性（字符长度）
	 * 
	 * @return 若字段数据类型有长度属性，则返回true
	 */
	public boolean hasStrLength() {
		return this.strLength != null;
	}

	/**
	 * 字段数据类型是否有长度属性（数字长度）
	 * 
	 * @return 若字段数据类型有长度属性，则返回true
	 */
	public boolean hasPrecision() {
		return this.precision != null && this.precision != 0;
	}

	/**
	 * 字段数据类型是否有精度属性（数字长度）
	 * 
	 * @return 若字段数据类型有精度属性，则返回true
	 */
	public boolean hasScale() {
		return this.scale != null && this.scale != 0;
	}

	/**
	 * 字段是否有默认值配置
	 * 
	 * @return 若字段有默认值配置，则返回true
	 */
	public boolean hasDefault() {
		return !StringUtil.isBlank(this.defaultDefine);
	}

	/**
	 * 判断字段的数据类型是否不为text
	 * 
	 * @return 若字段的数据类型不为text，则返回true
	 */
	public boolean notTextType() {
		if (StringUtil.isBlank(this.dataType)) {
			return true;
		}

		return this.dataType.toLowerCase().indexOf("text") == -1;
	}

	/**
	 * 判断字段的数据类型是否不为blob
	 * 
	 * @return 若字段的数据类型不为blob，则返回true
	 */
	public boolean notBlobType() {
		if (StringUtil.isBlank(this.dataType)) {
			return true;
		}

		return this.dataType.toLowerCase().indexOf("blob") == -1;
	}

	/**
	 * 判断字段的数据类型是否不为clob
	 * 
	 * @return 若字段的数据类型不为clob，则返回true
	 */
	public boolean notClobType() {
		if (StringUtil.isBlank(this.dataType)) {
			return true;
		}

		return this.dataType.toLowerCase().indexOf("clob") == -1;
	}

	/**
	 * 判断字段的数据类型是否为text
	 * 
	 * @return 若字段的数据类型为text，则返回true
	 */
	public boolean isTextType() {
		return !StringUtil.isBlank(this.dataType) && this.dataType.toLowerCase().indexOf("text") != -1;
	}

	/**
	 * 判断字段的数据类型是否不为int
	 * 
	 * @return 若字段的数据类型不为int，则返回true
	 */
	public boolean notIntType() {
		if (StringUtil.isBlank(this.dataType)) {
			return true;
		}

		return this.dataType.toLowerCase().indexOf("int") == -1;
	}

	/**
	 * 判断字段的数据类型是否为datetime
	 * 
	 * @return 若字段的数据类型为datetime，则返回true
	 */
	public boolean isDateTimeType() {
		if (StringUtil.isBlank(this.dataType)) {
			return false;
		}

		return this.dataType.equalsIgnoreCase("datetime");
	}

	/**
	 * 比较注释、名字、数据类型、是否为空
	 * 
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Column other = (Column) obj;
		if (StringUtil.isBlank(columnComment)) {
			if (!StringUtil.isBlank(other.getColumnComment()))
				return false;
		} else if (!StringUtil.isBlank(columnComment) && !StringUtil.isBlank(other.getColumnComment()) && !columnComment.equals(other.getColumnComment()))
			return false;
		if (StringUtil.isBlank(columnName)) {
			if (!StringUtil.isBlank(other.getColumnName()))
				return false;
		} else if (!columnName.equals(other.getColumnName()))
			return false;
		if (StringUtil.isBlank(dataType)) {
			if (!StringUtil.isBlank(other.getDataType()))
				return false;
		} else if (!dataType.toLowerCase().equals(other.getDataType().toLowerCase())) {
			return false;
		}
		if (nullAble != other.isNullAble())
			return false;
//		if (strLength != null && other.getStrLength() != null && !strLength.equals(other.getStrLength()))
//			return false;
//		if (precision != null && other.getPrecision() != null && !precision.equals(other.getPrecision()))
//			return false;
//		if (scale != null && other.getScale() != null && !scale.equals(other.getScale()))
//			return false;
		return true;
	}
	

}
