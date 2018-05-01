package bayuedekui.test;

import bayuedekui.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;

public class JDBCHelperTest {
    public static void main(String[] args) throws SQLException {
        //已经获得了数据可得连接(包括驱动的注册),在新建JDBCHelper对象的时候就已经将数据的注册,已经全部搞好(采用数据池的方式)
        //等执行具体的方法的时候再去pst,和conn
        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        
//       int res=jdbcHelper.executeUpdate("insert into test_user(name,age) values(?,?)",new Object[]{"sst",26});
//        System.out.println(res);
        jdbcHelper.executeQuery("select * from test_user where age>?", 
                new Object[]{5}, new JDBCHelper.queryCallback() {
            @Override//今天内部类
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    String name=rs.getString(2);
                    int age=rs.getInt(3);
                    System.out.println("name:"+name+" age:"+age);
                }
            }
        });
        
        
        //批量查询
        String sql="insert into test_user(name,age) values(?,?)";
        LinkedList<Object[]> paramList=new LinkedList<Object[]>();
        paramList.add(new Object[]{"zhegnkui",25});
        paramList.add(new Object[]{"liuwenchagn",24});
        int[] ints = jdbcHelper.executeBatch(sql, paramList);
        System.out.println(Arrays.toString(ints));

    }
}
