package bayuedekui.sparkproject.dao;

import bayuedekui.sparkproject.domain.SessionDetail;

import java.sql.SQLException;

/**
 * session明细DAO接口
 */
public interface ISessionDetailDAO {
    void insert(SessionDetail sessionDetail) throws SQLException;
}
