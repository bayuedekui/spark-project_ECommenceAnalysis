package bayuedekui.sparkproject.dao;

import bayuedekui.sparkproject.domain.Task;

import java.sql.SQLException;

/**
 * 任务管理dao接口
 */
public interface ITaskDao {
    /**
     * 根据主键查询任务 
     * @param taskid 主键
     * @return任务
     */
    Task findById(long taskid) throws SQLException;
}
