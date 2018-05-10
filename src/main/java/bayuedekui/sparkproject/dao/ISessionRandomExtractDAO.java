package bayuedekui.sparkproject.dao;

import bayuedekui.sparkproject.domain.SessionRandomExtract;

import java.sql.SQLException;

/**
 * session随机抽取模块DAO接口
 */
public interface ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     */
    void insert(SessionRandomExtract sessionRandomExtract) throws SQLException;
}
