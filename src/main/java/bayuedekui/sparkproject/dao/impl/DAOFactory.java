package bayuedekui.sparkproject.dao.impl;

import bayuedekui.sparkproject.dao.ITaskDao;

public class DAOFactory {
    /**
     * 获取任务管理的dao
     * @return DAO 
     */
    public static ITaskDao getTaskDAO(){
        return new TaskDAOImpl();
    }
}
