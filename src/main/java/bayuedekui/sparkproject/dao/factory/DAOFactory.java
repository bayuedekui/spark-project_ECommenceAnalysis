package bayuedekui.sparkproject.dao.factory;

import bayuedekui.sparkproject.dao.*;
import bayuedekui.sparkproject.dao.impl.*;

/**
 * 工厂类,用于创建工厂,实现的抽象化,有利于开闭原则
 */
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
    
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }
    
    public static ISessionDetailDAO getSessionDetilDAO(){
        return new SessionDetailDAOImpl();
    }
    
    public static ITop10CategoryDAO getTop10CategoryDAP(){
        return new Top10CategoryDAOImpl();
    }
    
    public static ITop10SesssionDAO getTop10SessionDAO(){
        return new Top10SessionImpl();
    }
}
