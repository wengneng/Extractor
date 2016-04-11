/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ForeignKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Partition;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.PrimaryKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ProcedureColumn;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Trigger;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;

/**
 * SQL Server数据字典采集实现
 * @author fbchen
 * @version 2.2 2010-10-18
 */
public class SQLServerExtractServiceImpl extends DBExtractBaseServiceImpl {
	private static Log log = LogFactory.getLog(SQLServerExtractServiceImpl.class);
	
	static final Pattern P_SYNCOBJ = Pattern.compile("^syncobj_0x\\p{XDigit}{16}", Pattern.CASE_INSENSITIVE);

	/*
	 * (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl#getSchemas()
	 */
	protected List<Schema> getSchemas() throws SQLException {
		log.info("开始采集Schema信息...");
		try {
			log.info("以JDBC方式采集表的信息...");
			String sql = "SELECT NAME FROM [sys].[schemas] s";
			final List<Schema> schemas = new ArrayList<Schema>();
			super.getJdbcTemplate().query(sql, new Object[] {}, new RowCallbackHandler(){
				public void processRow(ResultSet rs) throws SQLException {
					String schName = rs.getString("NAME");
					if (isMatchExtractSchema(schName)) { //过滤不需采集的Schema
						Schema schema = new Schema();
						schema.setName(schName);
						schemas.add(schema);
					}
				}
			});
			return schemas;
		} catch (DataAccessException e) {
			log.error("采用JDBC采集[Schema]失败，将以默认采集方式查询", e);
		}
		return super.getSchemas();
	}

	/**
	 * 过滤无效的表名，如：VT6MFTDKJ4L7J0PYLFIA1BU7WCS4X19KO，规律是：
	 * 表名长度31~~37，中间不带下划线，以VT6、VT8、VTZ、ZTZ开头
	 * @param tables 采集到的表
	 * @return 过滤后的表
	 */
	protected List<NamedColumnSet> filterTables(List<NamedColumnSet> tables) {
		for (Iterator<NamedColumnSet> it = tables.iterator(); it.hasNext(); ) {
			String name = it.next().getName();
			if (name.length() >= 31 && name.indexOf('_') == -1) {
				it.remove();
			}
		}
		return tables;
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #getTables(java.lang.String)
	 */
	protected List<NamedColumnSet> getTables(final String schName) throws SQLException {
		log.info("开始采集表信息...");
		try {
			log.info("以JDBC方式采集表的信息...");
			String sql = 
				"SELECT CAST(o.name as VARCHAR(100)) as TABLE_NAME\n" + 
				"    ,CAST(o.type_desc as VARCHAR(100)) as TABLE_TYPE\n" + 
				"    ,o.create_date\n" + 
				"    ,CAST(p.value as VARCHAR(1000)) AS REMARKS\n" + 
				"    ,o.IS_MS_SHIPPED\n" + 
				"FROM sys.tables o\n" + 
				"LEFT OUTER JOIN sys.extended_properties p\n" + 
				"    ON p.major_id=o.object_id and p.minor_id=0 and p.name in ('comment','MS_Description')\n" + 
				"WHERE o.schema_id = SCHEMA_ID(?)";
			final List<NamedColumnSet> tables = new ArrayList<NamedColumnSet>();
			final Set<String> tableNames = new HashSet<String>();
			super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
				public void processRow(ResultSet rs) throws SQLException {
					String name = rs.getString(Table.TABLE_NAME);
					if (!tableNames.contains(name)) {
						Table table = new Table();
						table.setName(name);
						table.setType(NamedColumnSetType.TABLE);
						table.addAttr(Table.TABLE_TYPE, rs.getString(Table.TABLE_TYPE));
						table.addAttr(Table.TABLESPACE_NAME, "");//rs.getString(Table.TABLESPACE_NAME)
						table.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
						table.addAttr("isSys", String.valueOf("1".equals(rs.getString("IS_MS_SHIPPED"))));
						table.addAttr("isTemp", String.valueOf(table.getName().startsWith("#")));
						table.addAttr("owner", schName); //rs.getString("OWNER_NAME")
						table.addAttr("nextExtent", "");
						table.addAttr("initialExtent", ""); //发现这个数总变，故去掉
						tables.add(table);
						tableNames.add(name);
					}
				}
			});
			return this.filterTables(tables);
		} catch (DataAccessException e) {
			log.error("采用JDBC采集[表]失败，将以默认采集方式查询", e);
		}
		
		List<NamedColumnSet> tables2 = super.getTables(schName);
		return this.filterTables(tables2);
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #getViews(java.lang.String)
	 */
	protected List<NamedColumnSet> getViews(String schName) throws SQLException {
		log.info("开始采集视图信息...");
		try {
			log.info("以JDBC方式采集视图的信息...");
			String sql = 
				"SELECT CAST(o.name as VARCHAR(100)) AS VIEW_NAME\n" +
				"    ,CAST(o.type_desc as VARCHAR(100)) as TABLE_TYPE\n" + 
				"    ,CAST(OBJECTPROPERTYEX(object_id,'IsIndexed') as integer) AS IS_INDEXED\n" + 
				"    ,CAST(OBJECTPROPERTYEX(object_id,'IsIndexable') as integer) AS IS_INDEXABLE\n" + 
				"    ,create_date\n" + 
				"    ,modify_date\n" + 
				"    ,CAST(p.value as VARCHAR(1000)) AS REMARKS\n" + 
				"FROM sys.views o\n" + 
				"LEFT OUTER JOIN [sys].[extended_properties] AS p\n" + 
				"    ON p.major_id=o.object_id and p.minor_id=0 and p.name in ('comment','MS_Description')\n" + 
				"WHERE o.schema_id = SCHEMA_ID(?)";
			final List<NamedColumnSet> views = new ArrayList<NamedColumnSet>();
			super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
				public void processRow(ResultSet rs) throws SQLException {
					String name = rs.getString(View.VIEW_NAME);
					if (!isFiltered(name)) {
						View view = new View();
						view.setName(name);
						view.setType(NamedColumnSetType.VIEW);
						view.setTableType(rs.getString(Table.TABLE_TYPE));
						view.addAttr(Table.TABLE_TYPE, rs.getString(Table.TABLE_TYPE));
						view.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
						views.add(view);
					}
				}
				
				// 被过滤的的名称，如syncobj_0x3838423039384346
				private boolean isFiltered(String name) {
					boolean filter = P_SYNCOBJ.matcher(name).matches();
					return filter;
				}
			});
			return views;
		} catch (DataAccessException e) {
			log.error("采用JDBC采集[视图]失败，将以默认采集方式查询", e);
		}
		return super.getViews(schName);
	}

	/*
	 * (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setViewText(java.lang.String, java.util.Map)
	 */
	protected void setViewText(String schName, final Map<String, View> viewCache) throws SQLException {
		log.info("开始采集视图的创建SQL...");
		try {
			String sql = 
				"SELECT CAST(o.name as VARCHAR(100)) AS VIEW_NAME\n" +
				"    ,m.definition as SQL\n" + 
				"FROM sys.views o\n" + 
				"INNER JOIN [sys].[sql_modules] m ON o.object_id = m.object_id\n" + 
				"WHERE o.schema_id = SCHEMA_ID(?)";
			super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
				public void processRow(ResultSet rs) throws SQLException {
					String name = rs.getString(View.VIEW_NAME);
					if (viewCache.containsKey(name)) {
						View view = viewCache.get(name);
						view.addAttr(View.SQL, rs.getString(View.SQL));
					}
				}
			});
		} catch (DataAccessException e) {
			log.error("采集视图的创建SQL失败", e);
		}
	}

	protected void setTableColumns(final String schName, final Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集表的字段信息...");
		try {
			log.info("以JDBC方式采集表的字段信息...");
			String sql = 
				"SELECT CAST(o.name as VARCHAR(1000)) AS TABLE_NAME\n" +
				"    ,CAST(c.name as VARCHAR(100)) AS COLUMN_NAME\n" +
				"    ,c.column_id as ORDINAL_POSITION\n" + 
				"    ,c.PRECISION as DECIMAL_PRECISION\n" + 
				"    ,c.SCALE as DECIMAL_DIGITS\n" + 
				"    ,c.system_type_id AS SQL_DATA_TYPE\n" + 
				"    ,c.user_type_id AS DATA_TYPE\n" + 
				"    ,CAST(TYPE_NAME(c.user_type_id) as VARCHAR(100)) AS TYPE_NAME\n" + 
				"    ,c.IS_NULLABLE\n" + 
				"    ,COALESCE(c.max_length,c.PRECISION) AS COLUMN_SIZE\n" +
				"    ,CAST(d.definition as VARCHAR(1000)) AS COLUMN_DEF\n" + 
				"    ,CAST(p.value as VARCHAR(1000)) AS REMARKS\n" + 
				"FROM sys.columns AS c\n" + 
				"INNER JOIN sys.tables AS o ON c.object_id=o.object_id\n" + 
				"LEFT OUTER JOIN [sys].[default_constraints] AS d ON d.object_id=c.default_object_id\n" + 
				"LEFT OUTER JOIN [sys].[extended_properties] AS p\n" +
				"     ON c.object_id=p.major_id and c.column_id=p.minor_id and p.name in ('comment','MS_Description')\n" + 
				"WHERE o.schema_id = SCHEMA_ID(?)";
			super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
				public void processRow(ResultSet rs) throws SQLException {
					String tableName = rs.getString(Table.TABLE_NAME);
					if (!tableCache.containsKey(tableName)) { return; }
					Column column = new Column();
					column.setName(rs.getString(Column.COLUMN_NAME));
					column.setTypeName(rs.getString(Column.TYPE_NAME));
					column.setDataType(rs.getInt(Column.DATA_TYPE));
					column.setColumnSize(rs.getInt(Column.COLUMN_SIZE));
					column.setCharOctetLength(rs.getInt(Column.DECIMAL_PRECISION)); //precision=八位字节、八位组?
					column.setDecimalDigits(rs.getInt(Column.DECIMAL_DIGITS));
					column.setSqlDataType(rs.getInt(Column.SQL_DATA_TYPE));
					column.setOrdinalPosition(rs.getInt(Column.ORDINAL_POSITION));
					column.setNullable("1".equals(rs.getString(Column.IS_NULLABLE)));
					column.setColumnDef(rs.getString(Column.COLUMN_DEF)); //默认值
					column.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
					column.setFormat(null);
					column.setCharacterSetName(null);
					column.setCompressible(null);
					column.setBufferLength(0); //rs.getInt(Column.BUFFER_LENGTH)
					column.setNumPrecRadix(10); //rs.getInt(Column.NUM_PREC_RADIX)
					column.setSqlDatetimeSub(0); //rs.getInt(Column.SQL_DATETIME_SUB)
					tableCache.get(tableName).addColumn(column);
				}
			});
			return;
		} catch (DataAccessException e) {
			log.error("采用JDBC采集表的字段失败，将以默认采集方式查询字段", e);
		}

		super.setTableColumns(schName, tableCache);
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setViewColumns(java.lang.String, java.util.Map)
	 */
	protected void setViewColumns(final String schName, final Map<String, View> viewCache) throws SQLException {
		log.info("开始采集视图的字段信息...");
		try {
			log.info("以JDBC方式采集视图的字段信息...");
			String sql = 
				"SELECT CAST(o.name as VARCHAR(1000)) AS TABLE_NAME\n" +
				"    ,CAST(c.name as VARCHAR(100)) AS COLUMN_NAME\n" +
				"    ,c.column_id as ORDINAL_POSITION\n" + 
				"    ,c.PRECISION as DECIMAL_PRECISION\n" + 
				"    ,c.SCALE as DECIMAL_DIGITS\n" + 
				"    ,c.system_type_id AS SQL_DATA_TYPE\n" + 
				"    ,c.user_type_id AS DATA_TYPE\n" + 
				"    ,CAST(TYPE_NAME(c.user_type_id) as VARCHAR(100)) AS TYPE_NAME\n" + 
				"    ,c.IS_NULLABLE\n" + 
				"    ,COALESCE(c.max_length,c.PRECISION) AS COLUMN_SIZE\n" +
				"    ,CAST(d.definition as VARCHAR(1000)) AS COLUMN_DEF\n" + 
				"    ,CAST(p.value as VARCHAR(1000)) AS REMARKS\n" + 
				"FROM sys.columns AS c\n" + 
				"INNER JOIN sys.views AS o ON c.object_id=o.object_id\n" + 
				"LEFT OUTER JOIN [sys].[default_constraints] AS d ON d.object_id=c.default_object_id\n" + 
				"LEFT OUTER JOIN [sys].[extended_properties] AS p\n" +
				"     ON c.object_id=p.major_id and c.column_id=p.minor_id and p.name in ('comment','MS_Description')\n" + 
				"WHERE o.schema_id = SCHEMA_ID(?)";
			super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
				public void processRow(ResultSet rs) throws SQLException {
					String viewName = rs.getString(Table.TABLE_NAME);
					if (!viewCache.containsKey(viewName)) { return; }
					Column column = new Column();
					column.setName(rs.getString(Column.COLUMN_NAME));
					column.setTypeName(rs.getString(Column.TYPE_NAME));
					column.setDataType(rs.getInt(Column.DATA_TYPE));
					column.setColumnSize(rs.getInt(Column.COLUMN_SIZE));
					column.setCharOctetLength(rs.getInt(Column.DECIMAL_PRECISION)); //precision
					column.setDecimalDigits(rs.getInt(Column.DECIMAL_DIGITS));
					column.setSqlDataType(rs.getInt(Column.SQL_DATA_TYPE));
					column.setOrdinalPosition(rs.getInt(Column.ORDINAL_POSITION));
					column.setNullable("1".equals(rs.getString(Column.IS_NULLABLE)));
					column.setColumnDef(rs.getString(Column.COLUMN_DEF)); //默认值
					column.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
					column.setBufferLength(0); //rs.getInt(Column.BUFFER_LENGTH)
					column.setNumPrecRadix(10); //rs.getInt(Column.NUM_PREC_RADIX)
					column.setSqlDatetimeSub(0); //rs.getInt(Column.SQL_DATETIME_SUB)
					viewCache.get(viewName).addColumn(column);
				}
			});
			return;
		} catch (DataAccessException e) {
			log.error("采用JDBC采集视图的字段失败，将以默认采集方式查询字段", e);
		}
		
		super.setViewColumns(schName, viewCache);
	}


	/**
	 * 获取表的主键
	 * 
	 * @param schName Schema
	 * @param tableCache 表
	 */
	protected void setPrimaryKeies(String schName, final Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集主键信息...");
		String sql = 
			"SELECT i.name AS PK_NAME, ic.INDEX_COLUMN_ID," +
			"   i.index_id AS KEY_SEQ, c.name AS COLUMN_NAME, t.name AS TABLE_NAME\n" + 
			"FROM sys.indexes AS i\n" + 
			"INNER JOIN sys.index_columns AS ic\n" + 
			"    ON i.object_id = ic.object_id AND i.index_id = ic.index_id\n" + 
			"INNER JOIN sys.columns AS c\n" + 
			"    ON ic.object_id = c.object_id AND c.column_id = ic.column_id\n" + 
			"INNER JOIN sys.objects AS t\n" + 
			"    ON c.object_id=t.object_id\n" + 
			"WHERE i.is_primary_key = 1\n" + 
			"  AND t.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (tableCache.containsKey(tableName)) {
					PrimaryKey primaryKey = new PrimaryKey();
					primaryKey.setName(rs.getString(PrimaryKey.PK_NAME));
					primaryKey.setColumnName(rs.getString(Column.COLUMN_NAME));
					primaryKey.addAttr(PrimaryKey.KEY_SEQ, String.valueOf(rs.getInt(PrimaryKey.KEY_SEQ)));

					Table table = tableCache.get(tableName);
					table.addPrimaryKey(primaryKey);
				}
			}
		});
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setForeignKeies(java.lang.String, java.util.Map)
	 */
	protected void setForeignKeies(final String schName, final Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集外键信息...");
		String sql = 
			"SELECT \n" +
			"    f.name AS FK_NAME\n" + 
			"   ,OBJECT_NAME(f.parent_object_id) AS TABLE_NAME\n" + 
			"   ,COL_NAME(fc.parent_object_id, fc.parent_column_id) AS FKCOLUMN_NAME\n" + 
			"   ,OBJECT_NAME (f.referenced_object_id) AS PKTABLE_NAME\n" + 
			"   ,COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS PKCOLUMN_NAME\n" + 
			"   ,is_disabled, f.key_index_id AS KEY_SEQ\n" + 
			"   ,delete_referential_action_desc\n" + 
			"   ,update_referential_action_desc\n" + 
			"FROM sys.foreign_keys AS f\n" + 
			"INNER JOIN sys.foreign_key_columns AS fc\n" + 
			"   ON f.object_id = fc.constraint_object_id\n" + 
			"WHERE f.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (tableCache.containsKey(tableName)) {
					ForeignKey fk = new ForeignKey();
					String name = rs.getString(ForeignKey.FK_NAME);
					if (name == null || name.equals("")) {
						name = new StringBuilder("FK.").append(rs.getString(ForeignKey.FKCOLUMN_NAME)).append("_").append(
								rs.getString(ForeignKey.PKCOLUMN_NAME)).toString();
					}
					fk.setName(name);
					fk.setFKColumnName(rs.getString(ForeignKey.FKCOLUMN_NAME));

					fk.setPKColumnName(rs.getString(ForeignKey.PKCOLUMN_NAME));
					fk.setPKTableName(rs.getString(ForeignKey.PKTABLE_NAME));
					fk.setPKSchemaName(schName);

					fk.addAttr(ForeignKey.KEY_SEQ, String.valueOf(rs.getInt(ForeignKey.KEY_SEQ)));
					Table table = tableCache.get(tableName);
					table.addForeignKey(fk);
				}
			}
		});
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setPartitions(java.lang.String, java.util.Map)
	 */
	protected void setPartitions(String schName, final Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集表分区...");
		String sql = 
			"SELECT OBJECT_NAME(p.object_id) AS TABLE_NAME\n" +
			"    ,p.PARTITION_ID AS PARTITION_NAME\n" + 
			"    ,i.name AS INDEX_NAME\n" + 
			"    ,p.partition_number as PARTITION_POSITION\n" + 
			"    ,p.ROWS\n" + 
			"FROM sys.partitions AS p\n" + 
			"INNER JOIN sys.indexes AS i ON p.object_id = i.object_id AND p.index_id = i.index_id\n" + 
			"INNER JOIN sys.objects AS o ON o.object_id = i.object_id\n" + 
			"LEFT OUTER JOIN sys.partition_schemes ps ON i.data_space_id=ps.data_space_id\n" + 
			"where o.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (tableCache.containsKey(tableName)) {
					Partition partition = new Partition();
					partition.setName(rs.getString(Partition.PARTITION_NAME));
					partition.setTableName(tableName);
					partition.addAttr(Partition.COMPOSITE, null);
					partition.addAttr(Partition.PARTITION_POSITION, rs.getString(Partition.PARTITION_POSITION));
					partition.addAttr(Partition.TABLESPACE_NAME, ""); //rs.getString(Partition.TABLESPACE_NAME)
					tableCache.get(tableName).addPartition(partition);
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setConstraints(java.lang.String, java.util.Map)
	 */
	protected void setConstraints(String schName, Map<String, Table> tableCache) throws SQLException {
		/*log.info("开始采集表级约束...");
		String sql = 
			"SELECT CAST(OBJECT_NAME(o.object_id) as VARCHAR(100)) as CONSTRAINT_NAME\n" +
			"    ,CAST(OBJECT_NAME(o.parent_object_id) as VARCHAR(100)) AS TABLE_NAME\n" + 
			"    ,CAST(o.type_desc as VARCHAR(100)) as type_desc\n" + 
			"    ,create_date\n" + 
			"    ,modify_date\n" + 
			"FROM sys.objects o\n" + 
			"WHERE type_desc LIKE '%CONSTRAINT'\n" + 
			"  AND o.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (tableCache.containsKey(tableName)) {
					CheckConstraint constraint = new CheckConstraint();
					constraint.setTables(new ArrayList<Table>());
					constraint.setConstraintColumns(new ArrayList<ConstraintColumn>());
					String name = rs.getString(ForeignKey.FK_NAME);
					constraint.setName(name);
					constraint.getTables().add(null); //TODO 设置表
					Table table = tableCache.get(tableName);
					table.addConstraint(constraint); //TODO //设置约束
				}
			}
		});*/
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #getIndexs(java.lang.String)
	 */
	protected List<SQLIndex> getIndexs(String schName) throws SQLException {
		log.info("开始采集索引信息..."); //由于父类未实现具体方法，则在此实现
		String sql = 
			"SELECT CAST(i.name as VARCHAR(1000)) AS INDEX_NAME\n" +
			"    ,CAST(COL_NAME(c.object_id,c.column_id) as VARCHAR(100)) as COLUMN_NAME\n" + 
			"    ,CAST(OBJECT_NAME(c.object_id) as VARCHAR(100)) as TABLE_NAME\n" + 
			"    ,CAST(SCHEMA_NAME(o.schema_id) as VARCHAR(100)) as SCH_NAME\n" +
			"    ,CAST(i.type_desc as VARCHAR(100)) as INDEX_TYPE\n" + 
			"    ,CAST(ds.type_desc as VARCHAR(1000)) AS filegroup_or_partition_scheme\n" + 
			"    ,CAST(ds.name as VARCHAR(100)) AS filegroup_or_partition_scheme_name\n" + 
			"    ,ignore_dup_key\n" + 
			"    ,IS_PRIMARY_KEY\n" + 
			"    ,IS_UNIQUE\n" + 
			"    ,IS_UNIQUE_CONSTRAINT\n" + 
			"    ,fill_factor\n" + 
			"    ,is_padded\n" + 
			"    ,is_disabled\n" + 
			"    ,allow_row_locks\n" + 
			"    ,allow_page_locks\n" + 
			"    ,c.key_ordinal AS ORDINAL_POSITION\n" + 
			"FROM sys.index_columns AS c\n" + 
			"INNER JOIN sys.indexes AS i ON c.object_id=i.object_id and c.index_id=i.index_id\n" + 
			"INNER JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id\n" + 
			"INNER JOIN sys.objects AS o ON i.object_id=o.object_id\n" + 
			"WHERE is_hypothetical = 0 AND i.index_id <> 0\n" + 
			"  AND o.schema_id = SCHEMA_ID(?)";
		final List<SQLIndex> indexs = new ArrayList<SQLIndex>();
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (!getCachedTables().containsKey(tableName)) {
					return; // 无效的表（如表名类似乱码的表已经被过滤了）
				}
				SQLIndex index = new SQLIndex();
				index.setName(rs.getString(SQLIndex.INDEX_NAME));
				index.setTableName(tableName);
				index.setColumnName(rs.getString(Column.COLUMN_NAME));
				index.setSchName(rs.getString(Schema.SCH_NAME));
				boolean isPrimaryKey = "1".equals(rs.getInt("IS_PRIMARY_KEY"));
				boolean isUnique = "1".equals(rs.getInt("IS_UNIQUE"));
				boolean isUniqueConstraint = "1".equals(rs.getInt("IS_UNIQUE_CONSTRAINT"));
				index.addAttr(SQLIndex.NON_UNIQUE, isPrimaryKey||isUnique ? "false" : "true");
				index.addAttr(SQLIndex.ORDINAL_POSITION, String.valueOf(rs.getInt(SQLIndex.ORDINAL_POSITION)));
				index.addAttr(SQLIndex.CARDINALITY, null); //String.valueOf(rs.getInt(SQLIndex.CARDINALITY))
				index.addAttr(SQLIndex.PAGES, null); //String.valueOf(rs.getInt(SQLIndex.PAGES))
				String rmk = isPrimaryKey ? "PRIMARY_KEY" : (isUniqueConstraint ? "UNIQUE_CONSTRAINT" : "");
				index.addAttr(SQLIndex.REMARKS, rmk); //
				index.addAttr(SQLIndex.INDEX_TYPE, rs.getString(SQLIndex.INDEX_TYPE));
				indexs.add(index);
			}
		});
		return indexs;
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #getProcedures(java.lang.String)
	 */
	protected List<Procedure> getProcedures(final String schName) throws SQLException {
		log.info("开始采集存储过程信息...");
		final List<Procedure> procedures = new ArrayList<Procedure>();
		
		// 查询所有的存储过程
		String sql1 = 
			"SELECT name AS PROCEDURE_NAME\n" +
			"    ,SCHEMA_NAME(schema_id) AS SCHEMA_NAME\n" + 
			"    ,type_desc as PROCEDURE_TYPE\n" + 
			"    ,create_date,modify_date\n" + 
			"FROM sys.procedures p\n" + 
			"WHERE p.schema_id = SCHEMA_ID(?)";

		// 查询所有的Function
		String sql2 = 
			"SELECT name AS PROCEDURE_NAME\n" +
			"  ,SCHEMA_NAME(schema_id) AS SCHEMA_NAME\n" + 
			"  ,type_desc as PROCEDURE_TYPE\n" + 
			"  ,create_date,modify_date\n" + 
			"FROM sys.objects f\n" + 
			"WHERE type_desc LIKE '%FUNCTION%'\n" + 
			"  and f.schema_id = SCHEMA_ID(?)";
		
		super.getJdbcTemplate().query(sql1, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				Procedure procedure = new Procedure();
				procedure.setName(rs.getString(Procedure.PROCEDURE_NAME));
				//procedure.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
				procedure.addAttr(Procedure.PROCEDURE_TYPE, rs.getString(Procedure.PROCEDURE_TYPE));
				procedures.add(procedure);
			}
		});
		super.getJdbcTemplate().query(sql2, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				Procedure procedure = new Procedure();
				procedure.setName(rs.getString(Procedure.PROCEDURE_NAME));
				//procedure.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
				procedure.addAttr(Procedure.PROCEDURE_TYPE, rs.getString(Procedure.PROCEDURE_TYPE));
				procedures.add(procedure);
			}
		});
		return procedures;
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setProcedureColumns(java.lang.String, java.util.Map)
	 */
	protected void setProcedureColumns(final String schName, final Map<String, Procedure> procedureCache) throws SQLException {
		log.info("开始采集存储过程或函数的参数信息...");
		String sql = 
			"SELECT p.parameter_id as SEQUENCE\n" +
			"    ,CAST(o.name as VARCHAR(1000)) AS PROCEDURE_NAME\n" + 
			"    ,CAST(o.TYPE_DESC as VARCHAR(100)) AS PROCEDURE_TYPE\n" + 
			"    ,CAST(p.name as VARCHAR(100)) AS COLUMN_NAME\n" + 
			"    ,p.user_type_id AS DATA_TYPE\n" + 
			"    ,CAST(TYPE_NAME(p.user_type_id) as VARCHAR(100)) AS TYPE_NAME \n" + 
			"    ,p.MAX_LENGTH as LENGTH\n" + 
			"    ,p.PRECISION\n" + 
			"    ,p.SCALE\n" + 
			"    ,CAST(p.DEFAULT_VALUE as VARCHAR(1000)) AS DEFAULT_VALUE\n" + 
			"    ,p.IS_OUTPUT as COLUMN_TYPE\n" + 
			"FROM sys.parameters AS p\n" + 
			"INNER JOIN sys.objects AS o ON o.object_id = p.object_id\n" + 
			"WHERE o.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String procName = rs.getString(Procedure.PROCEDURE_NAME);
				if (!procedureCache.containsKey(procName)) {
					return;
				}
				boolean isOutParam = rs.getInt(ProcedureColumn.COLUMN_TYPE) == 1;
				String sequence = String.valueOf(rs.getInt(ProcedureColumn.SEQUENCE));
				String pColumnName = rs.getString(ProcedureColumn.COLUMN_NAME);
				if (null == pColumnName || pColumnName.equals("")) {
					pColumnName = isOutParam ? "[out]" : "[in]" + sequence;
				}

				ProcedureColumn pColumn = new ProcedureColumn();
				pColumn.setName(pColumnName);

				pColumn.addAttr(ProcedureColumn.COLUMN_TYPE, String.valueOf(rs.getInt(ProcedureColumn.COLUMN_TYPE)));
				pColumn.addAttr(ProcedureColumn.DATA_TYPE, String.valueOf(rs.getInt(ProcedureColumn.DATA_TYPE)));
				pColumn.addAttr(ProcedureColumn.TYPE_NAME, rs.getString(ProcedureColumn.TYPE_NAME));
				pColumn.addAttr(ProcedureColumn.PRECISION, String.valueOf(rs.getInt(ProcedureColumn.PRECISION)));

				pColumn.addAttr(ProcedureColumn.LENGTH, String.valueOf(rs.getInt(ProcedureColumn.LENGTH)));
				pColumn.addAttr(ProcedureColumn.SCALE, String.valueOf(rs.getInt(ProcedureColumn.SCALE)));
				pColumn.addAttr(ProcedureColumn.NULLABLE, "true");
				pColumn.addAttr(ProcedureColumn.DEFAULT_VALUE, rs.getString(ProcedureColumn.DEFAULT_VALUE));
				pColumn.addAttr(ModelElement.REMARKS, isOutParam ? "OUT" : "IN"); //入参出参
				pColumn.addAttr(ProcedureColumn.SEQUENCE, sequence);// 参数顺序
				//pColumn.addAttr(ProcedureColumn.RADIX, String.valueOf(rs.getInt(ProcedureColumn.RADIX)));
				//pColumn.addAttr(ProcedureColumn.OVERLOAD, rs.getString(ProcedureColumn.OVERLOAD));
				procedureCache.get(procName).addPColumn(pColumn);
			}
		});
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #setProceduresText(java.lang.String, java.util.Map)
	 */
	protected void setProceduresText(final String schName, final Map<String, Procedure> procedureCache) throws SQLException {
		log.info("开始采集存储过程或函数的脚本文本...");
		String sql = 
			"select m.DEFINITION AS TEXT" +
			", o.name AS PROCEDURE_NAME" +
			", o.type_desc\n" +
			"from [sys].[sql_modules] m\n" + 
			"INNER JOIN sys.objects AS o ON o.object_id = m.object_id\n" + 
			"WHERE (o.type_desc like '%FUNCTION%' or o.type_desc like '%PROCEDURE%')\n" + 
			"  AND o.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String procName = rs.getString(Procedure.PROCEDURE_NAME);
				if (procedureCache.containsKey(procName)) {
					Procedure procedure = procedureCache.get(procName);
					String text = rs.getString(Procedure.TEXT);
					procedure.setText(text == null ? "" : text);
				}
			}
		});
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl
	 * #getTriggers(java.lang.String)
	 */
	protected List<Trigger> getTriggers(String schName) throws SQLException {
		// 由于父类未实现，因此这里实现
		log.info("开始采集触发器...");
		String sql = 
			"select CAST(t.name as VARCHAR(100)) AS TRIGGER_NAME\n" +
			"    ,CAST(t.type_desc as VARCHAR(100)) as TYPE_DESC\n" + 
			"    ,CAST(OBJECT_NAME(t.parent_id) as VARCHAR(100)) as TABLE_NAME\n" + 
			"    ,t.create_date\n" + 
			"    ,t.modify_date\n" + 
			"from sys.triggers t\n" + 
			"INNER JOIN sys.objects o ON o.object_id=t.object_id\n" + 
			"WHERE o.schema_id = SCHEMA_ID(?)";
		final List<Trigger> triggers = new ArrayList<Trigger>();
		final Map<String,Trigger> triggerCache = new HashMap<String,Trigger>();
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				Trigger trigger = new Trigger();
				trigger.setName(rs.getString(Trigger.TRIGGER_NAME));
				trigger.setText(null); //rs.getString(Trigger.TEXT)
				trigger.setRemarks(rs.getString("TYPE_DESC"));
				triggers.add(trigger);
				triggerCache.put(trigger.getName(), trigger);
			}
		});
		
		String sql2 = 
			"select CAST(t.name as VARCHAR(100)) AS TRIGGER_NAME\n" +
			"    ,CAST(e.type_desc as VARCHAR(100)) as EVENT_TYPE\n" + 
			"    ,e.IS_FIRST\n" + 
			"    ,e.IS_LAST\n" + 
			"from sys.triggers t\n" + 
			"INNER JOIN sys.trigger_events e ON t.object_id=e.object_id\n" + 
			"INNER JOIN sys.objects o ON o.object_id=t.object_id\n" + 
			"WHERE o.schema_id = SCHEMA_ID(?)";
		super.getJdbcTemplate().query(sql2, new Object[] { schName }, new RowCallbackHandler(){
			public void processRow(ResultSet rs) throws SQLException {
				String triggerName = rs.getString(Trigger.TRIGGER_NAME);
				Trigger trigger = triggerCache.get(triggerName);
				if (trigger != null) { // TODO 若同时响应Update、Delete时间，如何填写值?
					trigger.addAttr("Trigger_Fire", rs.getString("EVENT_TYPE"));
					boolean isFirst = "1".equals(rs.getString("IS_FIRST"));
					boolean isLast = "1".equals(rs.getString("IS_LAST"));
					trigger.addAttr("Trigger_When", isFirst ? "Before" : (isLast ? "After" : ""));
				}
			}
		});
		return triggers;
	}
	
	
}
