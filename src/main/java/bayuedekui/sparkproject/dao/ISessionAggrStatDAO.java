package bayuedekui.sparkproject.dao;

import bayuedekui.sparkproject.domain.SessionAggrStat;

import java.sql.SQLException;

/**
 * session聚合统计模块dao
 */
public interface ISessionAggrStatDAO {
    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat) throws SQLException;
}
