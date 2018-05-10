package bayuedekui.sparkproject.dao.impl;

import bayuedekui.sparkproject.dao.ISessionRandomExtractDAO;
import bayuedekui.sparkproject.domain.SessionRandomExtract;
import bayuedekui.sparkproject.jdbc.JDBCHelper;

import java.sql.SQLException;

/**
 * 随机抽取session的DAO实现
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) throws SQLException {
        String sql="insert into session_random_extract values(?,?,?,?,?)";
        Object[] params=new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
