package bayuedekui.test;

import java.sql.*;

public class jdbcCRUD {
    public static void main(String[] args) throws SQLException {
        insert();
        update();
    }
    public static void insert() throws SQLException {
        Connection conn=null;
        PreparedStatement st=null;
        try{
            //加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            conn= DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project?characterEncoding=utf8",
                    "root",
                    "123456"
            );
            String sql="insert into test_user(name,age) VALUES(?,?)";
            st=conn.prepareStatement(sql);
            st.setString(1,"zhengkui");
            st.setInt(2,99); 
            
            int res = st.executeUpdate();
            System.out.println(res);
            
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            if(st!=null){
                st.close();
            }
            if(conn!=null){
                conn.close();
            }
        } 
    }

    /**
     * 更新
     */
    public static void update() throws SQLException {
        Connection conn=null;
        Statement st=null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn=DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "123456"
            );
            st=conn.createStatement();
            String sql="update test_user set age=1 where name='张三'";
            int res=st.executeUpdate(sql);
            System.out.println(res==1?"update success":"update fail");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(st!=null){
                st.close();
            }
            if(conn!=null){
                conn.close();
            }
        }
    }
}
