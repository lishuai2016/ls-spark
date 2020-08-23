package project1.dao.impl;

import project1.dao.IPageSplitConvertRateDAO;
import project1.domain.PageSplitConvertRate;
import project1.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 * @author Administrator
 *
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {

	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";
		Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
				pageSplitConvertRate.getConvertRate()};

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
