package bayuedekui.sparkproject.dao;

import bayuedekui.sparkproject.domain.Top10Session;

import java.sql.SQLException;

/**
 * top10活跃session的DAO接口
 */
public interface ITop10SesssionDAO {
    void insert(Top10Session top10Session) throws SQLException;
}
