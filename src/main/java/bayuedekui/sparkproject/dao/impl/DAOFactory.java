package bayuedekui.sparkproject.dao.impl;

import bayuedekui.sparkproject.dao.ISessionAggrStatDAO;
import bayuedekui.sparkproject.dao.ITaskDao;

public class DAOFactory {
    /**
     * 获取任务管理的dao
     * @return DAO 
     */
    public static ITaskDao getTaskDAO(){
        return new TaskDAOImpl();
    }

    /**
     * 获取session聚合统计DAO
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }
}
