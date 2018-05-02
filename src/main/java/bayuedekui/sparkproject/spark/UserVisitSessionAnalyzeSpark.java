package bayuedekui.sparkproject.spark;

import bayuedekui.sparkproject.conf.ConfigurationManager;
import bayuedekui.sparkproject.constant.Constants;
import bayuedekui.sparkproject.dao.ITaskDao;
import bayuedekui.sparkproject.dao.impl.DAOFactory;
import bayuedekui.sparkproject.domain.Task;
import bayuedekui.sparkproject.util.ParamUtils;
import bayuedekui.sparkproject.util.StringUtils;
import bayuedekui.sparkproject.util.ValidUtils;
import bayuedekui.test.MockData;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.bcel.internal.classfile.ConstantString;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.columnar.LONG;
import org.apache.spark.sql.hive.HiveContext;
import py4j.StringUtil;
import scala.Tuple2;
import scala.tools.nsc.doc.DocFactory;

import java.sql.SQLException;
import java.util.Iterator;

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) throws SQLException {
      
  
       args=new String[]{"1"};
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());
        //调用方法,生成模拟数据
        mockData(sc, sqlContext);
        //创建需要使用的DAO组件
        ITaskDao taskDao = DAOFactory.getTaskDAO();

        long taskId = ParamUtils.getTaskIdFromArgs(args, "");
        Task task = taskDao.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());//将数据库中存taskParam字段的内容转成json格式的对象

        //如果需要进行session粒度的聚合,首先要从user_visit_action表中查出指定范围的的欣慰数据
        //如果要根据用户在创建任务时指定的参数,来进行数据的过滤和筛选,首先要查询出来指定任务,并获取任务的查询参数
        //拿到了指定范围内的数据
        JavaRDD<Row> actionRDD = getActionRDDByDataRange(sqlContext, taskParam);

        //下面开始第一步的聚合:首先按照session_ID进行groupByKey进行分组
        //此时的数据的粒度就是session粒度了,然后session粒度的数据与用户信息的数据进行join
        //然后就可以获得session粒度的数据,同时还包含了session对应的user的信息
        //通过以下方法,获得数据格式为<sessionid,fullAggrInfo(sessionid,searchKeyWords,clickCategoryIds,age,city,professional,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);

        System.out.println(sessionid2AggrInfoRDD.count());
        for(Tuple2<String,String> tuple:sessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple._2);
        }
        //接着对聚合好的sessionid2AggrInfoRDD数据进行过滤
        //相当于我们编写的算子函数,是要访问外面的任务参数对象的
        //规则:匿名内部类(算子函数),访问外部对象,是要给外部对象使用final修饰的
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD=filterSession(sessionid2AggrInfoRDD,taskParam);
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        for(Tuple2<String,String> tuple:filteredSessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple._2);
        }
        //关闭spark上下文
        sc.close();
    }
 
    /**
         * 如果是在本地测试那就生成SQLContext
         * 如果是在集群那就是在HiveContext
         *
         * @param sc SparkContext
         * @return SQLContext
         */
    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SAPRK_LOCAL);

        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
         * 生成本地数据(只有本地模式才生成模拟数据)
         *
         * @param sc
         * @return sqlContext
         */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SAPRK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
         * 获取指定日期范围内的用户访问行为数据
         *
         * @param sqlContext
         * @param taskParam
         * @return
         */
    private static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where date>='" + startDate + "'and date<='" + endDate + "'";
        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();//将从数据库查到的数据转成javaRDD
    }

    /**
         * 对行为数据按session粒度进行聚合
         *
         * @param actionRDD 行为数据RDD
         * @return session粒度聚合数据
         */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext) {
        //现在actionRDD中的元素是Row,一个row就是一行用户的访问行为记录(比如一次点击或者搜索)
        //我们现在需要将这个row映射成<sessionID,row>的格式
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(
                /**
         * 第一个row表示函数的输入,第二个第三个表示函数的输出(Tuple)
         */
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }
                });
        //对行为数据进行按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD.groupByKey();

        //对每一个session分组进行聚合,将session中所有的搜索词和点击品类都聚合起来
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        //将搜索词和点击品类数据提取出来
                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();
                        StringBuffer searchkeywordBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }

                            String searchKeyWords = row.getString(5);
                            Long clickCategoryIds = row.getLong(6);
                            //说明:并不是所有行为访问都同时具有searchkeyWord和clickCategory的
                            //所以任何一行行为数据都不可能同时又两个字段,数据是有可能出现null值的

                            //我们决定将搜索词或者品类ID拼接到字符串去
                            //需要满足:不能为null,之前字符窜没有搜索词或者品类ID
                            if (StringUtils.isNotEmpty(searchKeyWords)) {
                                if (!searchkeywordBuffer.toString().contains(searchKeyWords)) {
                                    {
                                        searchkeywordBuffer.append(searchKeyWords + ",");
                                    }
                                }
                            }

                            if (clickCategoryIds != null) {
//                              if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryIds))
//                                    {
                                clickCategoryIdsBuffer.append(clickCategoryIds + ",");
//                                     }
                            }
                        }
                        //去掉末尾的","
                        String searchKeyWords = StringUtils.trimComma(searchkeywordBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //因为我们要的是和用户表进行join,所以我们要把之前的<session_id,partAggrInfo>
                        //转化成<user_id,partAggrInfo>,还要再做一次maptopair算子(多此一举)
                        //因为可以直接定义返回的数据格式为<user_id,partAggrInfo>,然后和用户信息进行join
                        //将partAggrInfo关联上userInfo,再直接将返回的Tuple的key设置成sessionID
                        //最后的数据格式就是<session_id,partAggrInfo>

                        //聚合数据用什么样的格式拼接
                        //我们统一定义为key=value|key=value|......
                        String partAggrInfo = Constants.FILED_SESSION_ID + "=" + sessionId + "|" +
                                Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
                                Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                        //到此为止获取的数据格式就是<userID,partAggrInfo>
                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }
                });

        //下面是查询用户信息的操作(查询所有用户数据),并映射成为<userID,Row>的格式                
        String sqlUsr = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sqlUsr).javaRDD();
        //映射成<userID,Row>的数据格式
        JavaPairRDD<Long, Row> userId2toInfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                });
        //将session粒度聚合数据和用户信息进行join(不同方向的join,产生的结果数据格式不一样)
        JavaPairRDD<Long, Tuple2<String, Row>> userId2toFullInfoRDD = userid2PartAggrInfoRDD.join(userId2toInfoRDD);

        //对join起来的数据进行拼接,并返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> session2idFullInfoRDD = userId2toFullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        //获取userInfo我们需要的消息
                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        //从partAggrInfo中截取session_id(调用stringutils中的字符串切割方法)
                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, "\\|", Constants.FILED_SESSION_ID);


                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
                });
        return null;
    }//aggregateSession结束

    /**
         * 过滤已经聚合的session数据
         * @param sessionid2AggrInfoRDD
         * @return java
         */
    //当匿名内部类调用外部参数时,必须用final修饰
    private  static JavaPairRDD<String,String> filterSession(JavaPairRDD<String,String> sessionid2AggrInfoRDD,final JSONObject taskParam){
        //为了使用ValieUtils,我们首先将筛选参数拼成一个串,不要觉得多此一举,其实是为了性能优化埋下伏笔
        String startAge=ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge=ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals=ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities=ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex=ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords=ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categotyIds=ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
        
        String _parameter=(startAge!=null?Constants.PARAM_START_AGE+"="+startAge+"|":"")
                +(endAge!=null?Constants.PARAM_END_AGE+"="+endAge+"|":"")
                +(professionals!=null?Constants.PARAM_PROFESSIONALS+"="+professionals+"|":"")
                +(cities!=null?Constants.PARAM_CITIES+"="+cities+"|":"")
                +(sex!=null?Constants.PARAM_SEX+"="+sex+"|":"")
                +(keywords!=null?Constants.PARAM_KEYWORDS+"="+keywords+"|":"")
                +(categotyIds!=null?Constants.PARAM_CATEGORY_IDS+"="+categotyIds+"|":"");
        
        if(_parameter.endsWith("\\|")){
           _parameter=_parameter.substring(0,_parameter.length()-1); 
        }
        final String parameter=_parameter;
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD=sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //首先,从Tuple中获取聚合数据
                        String aggrInfo=tuple._2;
                        
                        //接着按照筛选条件进行筛选
                        //按照年龄范围进行过滤(采用ValidUtils工具类进行过滤)
                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
                            return false;
                        }
                        
                        //按照职业范围进行过滤,eg:互联网,IT,软件
                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,
                                parameter,Constants.PARAM_PROFESSIONALS)){
                            return false;
                        }
                        
                        //按照城市范围进行过滤eg:北京,上海,广东,深证
                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)){
                            return false;
                        }
                        
                        //按照性别进行过滤
                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEX,parameter, Constants.PARAM_SEX)){
                            return false;
                        }
                        
                        //按照搜索词过滤
                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEYWORDS,parameter,Constants.PARAM_KEYWORDS)){
                            return false;                            
                        }
                        
                        //按照点击品类id过滤
                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,parameter,Constants.PARAM_CATEGORY_IDS)){
                            return false;
                        }
                        return true;
                    }
        });
        
        return filteredSessionid2AggrInfoRDD;
    }
}


    
    
    
    
    
    
    
    
    
    
    
    

