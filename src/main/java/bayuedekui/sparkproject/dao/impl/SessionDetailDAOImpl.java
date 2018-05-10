package bayuedekui.sparkproject.dao.impl;


import bayuedekui.sparkproject.dao.ISessionDetailDAO;
import bayuedekui.sparkproject.domain.SessionDetail;
import bayuedekui.sparkproject.jdbc.JDBCHelper;

import java.sql.SQLException;

/**
 * session明细DAO实现类
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {
    /**
     * 插入一条session明细数据
     */
    @Override
    public void insert(SessionDetail sessionDetail) throws SQLException {
        String sql="insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params=new Object[]{sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPageid(),
                sessionDetail.getPayProductIds()
        };

        JDBCHelper jdbcHelper=new JDBCHelper();
        jdbcHelper.executeUpdate(sql,params);
    }
    
    
}
