package bayuedekui.test;

import bayuedekui.sparkproject.dao.ITaskDao;
import bayuedekui.sparkproject.dao.impl.DAOFactory;
import bayuedekui.sparkproject.domain.Task;

import java.sql.SQLException;

public class TaskDAOTest {
    public static void main(String[] args) throws SQLException {
        ITaskDao taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);//通过某一字段查到一整行的数据
        System.out.println(task.getTaskName());
        
    }
}
