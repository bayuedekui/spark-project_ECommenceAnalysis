package bayuedekui.sparkproject.dao;


import bayuedekui.sparkproject.domain.Top10Category;

import java.sql.SQLException;

/**
 * top10品类DAO接口
 */
public interface ITop10CategoryDAO {
    void insert(Top10Category category) throws SQLException;
}
