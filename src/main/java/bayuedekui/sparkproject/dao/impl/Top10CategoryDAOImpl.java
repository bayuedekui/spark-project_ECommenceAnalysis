package bayuedekui.sparkproject.dao.impl;

import bayuedekui.sparkproject.dao.ITop10CategoryDAO;
import bayuedekui.sparkproject.domain.Top10Category;
import bayuedekui.sparkproject.jdbc.JDBCHelper;

import java.sql.SQLException;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category category) throws SQLException {
        String sql="insert into top10_category values(?,?,?,?,?)";
        Object[] params=new Object[]{
                category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getCategoryid(),
                category.getPayCount()
        };

        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
