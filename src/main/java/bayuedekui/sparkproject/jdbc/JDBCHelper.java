package bayuedekui.sparkproject.jdbc;

import bayuedekui.sparkproject.conf.ConfigurationManager;
import bayuedekui.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;

public class JDBCHelper {
    /**
     * 第一步,静态方法注册驱动
     */
    static{
        try {
            String driver= ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
            
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            
        }
    }

    
    //第二部:实现JDBCHelper的单例化
    //因为内部要封装一个简单的内部的数据库连接池,为了保证数据库连接池有且仅有一份,所以通过单例模式
    //保证JDBCHelper只有一个实例,实例中只有一份数据库连接池
    private static JDBCHelper instance=null;

    
    //利用单例模式构造数据库连接的单例
    public static JDBCHelper getInstance(){
        if(instance==null){
            synchronized(JDBCHelper.class){//找到构造JDBCHelper实例的类
                if(instance==null){
                    instance=new JDBCHelper();
                }
            }
        }
        return instance;
    }

    
    LinkedList<Connection> datasource=null;    
    /**
     * 第三部(创造连接池):私有化构造方法,整个周期过程中只创建一次,
     * 所以可以去创建自己唯一的饿一个数据库连接池
     */
    public JDBCHelper(){
        //首先定义数据池的大小(可以通过配置文件灵活的饿获取)
        int datasourceSize=ConfigurationManager.getIntger(Constants.JDBC_DATASOURCE_SIZE);
        //然后创建指定数量的数据库连接,并放入数据库连接池中
        for(int i=0;i<datasourceSize;i++){
            String url=ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user=ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection conn= DriverManager.getConnection(
                        url+"characterEncoding=utf8",user,password);
                datasource.push(conn);//将获取到的连接放到队列中去
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 第四部:提供数据库连接
     * 有可能去获取的时候,里面的连接用完了,需要自己代码实现一个等待机制
     */
    public synchronized Connection getConection(){
        while(datasource.size()==0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();//队列中走出一个对象
    }

    /**
     * 第五:开发CRUD的方法
     * 1.增删改2.查3.批量执行sql语句的方法
     */
    //执行增删改的sql语句,返回影响的行数
    public int executeUpdate(String sql,Object[] arr) throws SQLException {
        int res=0;
        Connection conn=null;
        PreparedStatement pst=null;
        try {
            conn=getConection();
            pst=conn.prepareStatement(sql);
            for(int i=0;i<arr.length;i++){
                pst.setObject(i+1,arr[i]);
            }
            res= pst.executeUpdate();
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(pst!=null){
                pst.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
        return res ;
    }
    /**
     * 查询sql,构建内部接口或者内部类,传入对查到结果的操作
     */
    public void executeQuery(String sql,Object[] arr,queryCallback callback) throws SQLException {
        Connection conn=null;
        PreparedStatement pst=null;
        ResultSet rs=null;
        try {
            conn=getConection();
            pst=conn.prepareStatement(sql);
            for(int i=0;i<arr.length;i++){
                pst.setObject(i+1,arr[i]);
            }
             rs=pst.executeQuery();
            callback.process(rs);//回调函数,传进来外部处理数据的方法
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pst!=null) {
                pst.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
    }
    //内部类:查询回调接口
    static interface queryCallback{
        void process(ResultSet rs) throws Exception;
    }

    /**
     * 批量执行sql,虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
     * 都要向MySQL发送一次网络请求
     *
     * 可以通过批量执行SQL语句的功能优化这个性能
     * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
     * 执行的时候，也仅仅编译一次就可以
     * 这种批量执行SQL语句的方式，可以大大提升性能
     */
    
    public int[] executeBatch(String sql,LinkedList<Object[]> paramList) throws SQLException {
        int[] res=null;
        Connection conn=null;
        PreparedStatement pst=null;
        try {
            conn=getConection();
            //第一步,取消自动提交
            conn.setAutoCommit(false);
            pst=conn.prepareStatement(sql);
            //二:采用addBash()方式加入批次处理
            if(paramList!=null&&paramList.size()!=0){
                for(Object[] param:paramList) {//遍历队列中的批次sql
                    for (int i = 0; i < param.length; i++) {
                        pst.setObject(i+1,param[i]);
                    }
                    pst.addBatch();
                }
            }
            //三:使用preparedStatement.executeBatch()方法执行批量处理sql语句
            res=pst.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           if(conn!=null){
               datasource.push(conn);
           }
        }
    }


}
