package bayuedekui.sparkproject.dao.impl;

import bayuedekui.sparkproject.dao.ITaskDao;
import bayuedekui.sparkproject.domain.Task;
import bayuedekui.sparkproject.jdbc.JDBCHelper;
import org.apache.mesos.Protos;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TaskDAOImpl implements ITaskDao {
    @Override
    public Task findById(long taskid) throws SQLException {
        final Task task = new Task();
        String sql = "select * from task where task_id=?";
        Object[] params = new Object[]{taskid};
        
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.queryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskId(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });
    return task;
    }
}
