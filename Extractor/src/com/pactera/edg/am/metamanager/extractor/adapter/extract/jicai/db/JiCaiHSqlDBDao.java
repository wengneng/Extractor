/**    
  * @FileName: HSqlDBDao.java  
  * @Package:com.vibi.dgp.extractor.adapter.cgb.extract.db  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-10-10 上午11:33:58  
  * @version V1.0    
  */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;

/** 
  * @ClassName: HSqlDBDao  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-10-10 上午11:33:58   
  */
public interface JiCaiHSqlDBDao {
	
	public int executeQuery(String sql) throws SQLException;
	
	public int executeQuery(String sql,String [] param) throws SQLException;
	
	public List queryForList(String sql)throws SQLException;

	public List queryForList(String sql,String [] param)throws SQLException;
	
	public int queryForInt(String sql)throws SQLException;
	
	public int queryForInt(String sql,String [] param)throws SQLException;

	public List<TableVO> queryForListToTableVO(String sql,String [] param)throws SQLException;
	
 	public List<ColumnVO> queryForListToFieldVO(String sql,String [] param)throws SQLException;
	
	public Map query(String sql)throws SQLException;
	
	public Map query(String sql,String [] param)throws SQLException;
	
}
