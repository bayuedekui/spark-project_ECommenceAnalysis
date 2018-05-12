package bayuedekui.sparkproject.dao.impl;

import bayuedekui.sparkproject.dao.ITop10CategoryDAO;
import bayuedekui.sparkproject.dao.ITop10SesssionDAO;
import bayuedekui.sparkproject.domain.Top10Session;
import bayuedekui.sparkproject.jdbc.JDBCHelper;

import java.sql.SQLException;

public class Top10SessionImpl implements ITop10SesssionDAO {

    @Override
    public void insert(Top10Session top10Session) throws SQLException {
        String sql="insert into top10_session values(?,?,?,?)";
        
        Object[] params=new Object[]{
                top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()
        };

        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
