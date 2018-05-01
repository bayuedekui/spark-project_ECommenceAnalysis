package bayuedekui.sparkproject.spark;

import bayuedekui.sparkproject.conf.ConfigurationManager;
import bayuedekui.sparkproject.constant.Constants;
import bayuedekui.sparkproject.dao.ITaskDao;
import bayuedekui.sparkproject.dao.impl.DAOFactory;
import bayuedekui.sparkproject.domain.Task;
import bayuedekui.sparkproject.util.ParamUtils;
import bayuedekui.test.MockData;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.tools.nsc.doc.DocFactory;

import java.sql.SQLException;

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) throws SQLException {
        SparkConf conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=getSQLContext(sc.sc());    
        //调用方法,生成模拟数据
        mockData(sc,sqlContext);
        //创建需要使用的DAO组件
        ITaskDao taskDao= DAOFactory.getTaskDAO(); 

        //如果需要进行session粒度的聚合,首先要从user_visit_action表中查出指定范围的的欣慰数据
        //如果要根据用户在创建任务时指定的参数,来进行数据的过滤和筛选,首先要查询出来指定任务,并获取任务的查询参数
        long taskId= ParamUtils.getTaskIdFromArgs(args,"");
        Task task=taskDao.findById(taskId);
        JSONObject taskParam=JSONObject.parseObject(task.getTaskParam());//将数据库中存taskParam字段的内容转成json格式的对象
        sc.close();
    }
    /**
     * 如果是在本地测试那就生成SQLContext
     * 如果是在集群那就是在HiveContext
     * @param sc SparkContext
     * @return SQLContext
     */
    public static SQLContext getSQLContext(SparkContext sc){
        boolean local= ConfigurationManager.getBoolean(Constants.SAPRK_LOCAL);

        if(local){
            return new SQLContext(sc);
        }else{
            return new HiveContext(sc);
        }
    }
    /**
     * 生成本地数据(只有本地模式才生成模拟数据)
     * @param   sc
     * @return sqlContext
     */
    public static void mockData(JavaSparkContext sc,SQLContext sqlContext){
        boolean local=ConfigurationManager.getBoolean(Constants.SAPRK_LOCAL);
        if(local){
            MockData.mock(sc,sqlContext);
        }
    }
}
