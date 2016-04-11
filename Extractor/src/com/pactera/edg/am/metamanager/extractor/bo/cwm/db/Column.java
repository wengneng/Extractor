package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Attribute;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.DataType;

public class Column extends Attribute {

	/**
	 * 
	 */
	private static final long serialVersionUID = -161440878417068867L;

	public final static String COLUMN_NAME = "COLUMN_NAME";

	public final static String COLUMN_DEF = "COLUMN_DEF";

	public final static String TYPE_NAME = "TYPE_NAME";

	public final static String DATA_TYPE = "DATA_TYPE";

	public final static String COLUMN_SIZE = "COLUMN_SIZE";

	public final static String BUFFER_LENGTH = "BUFFER_LENGTH";

	public final static String DECIMAL_DIGITS = "DECIMAL_DIGITS";

	public final static String NUM_PREC_RADIX = "NUM_PREC_RADIX";

	public final static String SQL_DATA_TYPE = "SQL_DATA_TYPE";

	public final static String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";

	public final static String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";

	public final static String ORDINAL_POSITION = "ORDINAL_POSITION";

	public static final String IS_NULLABLE = "IS_NULLABLE";
	
	public final static String DECIMAL_PRECISION = "DECIMAL_PRECISION";

	// Column依赖于DataType
	private List<DataType> dataTypes;

	// Column依赖于CheckConstraint
	private List<CheckConstraint> constraints;

	private ColumnSet ownerColumnSet;

	private Boolean nullable;

	private String typeName;

	private String columnDef;

	private String remarks;

	private Integer dataType;

	private Integer columnSize;

	private Integer bufferLength;

	private Integer decimalDigits;

	private Integer numPrecRadix;

	private Integer sqlDataType;

	private Integer sqlDatetimeSub;

	private Integer charOctetLength;

	private Integer ordinalPosition;

	private String format;
	
	private Boolean compressible;
	
	private String characterSetName;
	
	/**
	 * <schema.table.column, NamedColumnSetType>
	 */
	private Map<String, NamedColumnSetType> referenceSchTableColumns = new HashMap<String, NamedColumnSetType>(0);

	private Boolean isPK;

	public ColumnSet getOwnerColumnSet() {
		return ownerColumnSet;
	}

	public void setOwnerColumnSet(ColumnSet ownerColumnSet) {
		this.ownerColumnSet = ownerColumnSet;
	}

	public List<DataType> getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(List<DataType> dataTypes) {
		this.dataTypes = dataTypes;
	}

	public List<CheckConstraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(List<CheckConstraint> constraints) {
		this.constraints = constraints;
	}

	public Boolean isNullable() {
		return nullable;
	}

	public void setNullable(Boolean nullable) {
		this.nullable = nullable;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getColumnDef() {
		return columnDef;
	}

	public void setColumnDef(String columnDef) {
		this.columnDef = columnDef;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public Integer getDataType() {
		return dataType;
	}

	public void setDataType(Integer dataType) {
		this.dataType = dataType;
	}

	public Integer getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(Integer columnSize) {
		this.columnSize = columnSize;
	}

	public Integer getBufferLength() {
		return bufferLength;
	}

	public void setBufferLength(Integer bufferLength) {
		this.bufferLength = bufferLength;
	}

	public Integer getDecimalDigits() {
		return decimalDigits;
	}

	public void setDecimalDigits(Integer decimalDigits) {
		this.decimalDigits = decimalDigits;
	}

	public Integer getNumPrecRadix() {
		return numPrecRadix;
	}

	public void setNumPrecRadix(Integer numPrecRadix) {
		this.numPrecRadix = numPrecRadix;
	}

	public Integer getSqlDataType() {
		return sqlDataType;
	}

	public void setSqlDataType(Integer sqlDataType) {
		this.sqlDataType = sqlDataType;
	}

	public Integer getSqlDatetimeSub() {
		return sqlDatetimeSub;
	}

	public void setSqlDatetimeSub(Integer sqlDatetimeSub) {
		this.sqlDatetimeSub = sqlDatetimeSub;
	}

	public Integer getCharOctetLength() {
		return charOctetLength;
	}

	public void setCharOctetLength(Integer charOctetLength) {
		this.charOctetLength = charOctetLength;
	}

	public Integer getOrdinalPosition() {
		return ordinalPosition;
	}

	public void setOrdinalPosition(Integer ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}

	public Boolean isPK() {
		return isPK;
	}

	public void setPK(Boolean isPK) {
		this.isPK = isPK;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public Boolean getCompressible() {
		return compressible;
	}

	public void setCompressible(Boolean compressible) {
		this.compressible = compressible;
	}

	public String getCharacterSetName() {
		return characterSetName;
	}

	public void setCharacterSetName(String characterSetName) {
		this.characterSetName = characterSetName;
	}

	public Map<String, NamedColumnSetType> getReferenceSchTableColumns() {
		return referenceSchTableColumns;
	}

	public void addReferenceSchTableColumn(String schTableColumn, NamedColumnSetType type) {
		referenceSchTableColumns.put(schTableColumn, type);

	}

}
