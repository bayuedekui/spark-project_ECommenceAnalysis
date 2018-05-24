package bayuedekui.sparkproject.spark.session;

import bayuedekui.sparkproject.conf.ConfigurationManager;
import bayuedekui.sparkproject.constant.Constants;
import bayuedekui.sparkproject.dao.*;
import bayuedekui.sparkproject.dao.factory.DAOFactory;
import bayuedekui.sparkproject.domain.*;
import bayuedekui.sparkproject.util.*;
import bayuedekui.test.MockData;
import com.alibaba.fastjson.JSONObject;
import groovy.lang.Category;
import groovy.lang.Tuple;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.awt.image.RasterOp;
import java.sql.SQLException;
import java.util.*;

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) throws SQLException {


        args = new String[]{"1"};
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());
        //调用方法,生成模拟数据
        mockData(sc, sqlContext);
        //创建需要使用的DAO组件
        ITaskDao taskDao = DAOFactory.getTaskDAO();

        long taskId = ParamUtils.getTaskIdFromArgs(args, "");
        //System.out.println("hahahahh"+taskId);
        Task task = taskDao.findById(taskId);
    //        System.out.println("oow-->"+task.getTaskName()+task.getTaskParam());
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());//将数据库中存taskParam字段的内容转成json格式的对象
       
        //如果需要进行session粒度的聚合,首先要从user_visit_action表中查出指定范围的行为数据     
        //如果要根据用户在创建任务时指定的参数,来进行数据的过滤和筛选,首先要查询出来指定任务,并获取任务的查询参数
        //拿到了指定范围内的数据

        /**重构:
         * sactionRDD就是一个公共的RDD
         * 第一,要用actionRDD,获取到一个公共的的sessionid为key的PairRDD
         * 第二,action用在了session聚合环节里面
         * 
         * sessionid为key的PairRDD,是确定了,在后面要用很多次
         * 1.与筛选的sessionid进行join,获取通过筛选的session的明细数据
         * 2.将这个RDD,直接传入aggregateBySession方法,进行session聚合统计
         * 
         * 重构完成后,actionRDD,就只在最开始使用一次,用来生成一sessionid为keys的RDD
         */
        JavaRDD<Row> actionRDD = getActionRDDByDataRange(sqlContext, taskParam);
        
        //将actionRDD映射成为<actionRDD,row>的格式
        JavaPairRDD<String ,Row> sessionid2actionRDD=getSessionid2ActionRDD(actionRDD);//sessionid2actionRDD在后面被用到两次,因此可以持久化

        /**
         * 持久化,很简单,就是对RDD调用persist()方法,并传入一个持久化级别
         * 
         * 如果是pesist(StorageLevel.MENORY_ONLY())纯内存,无序列化,那么可以用cache()方法来代替
         * StorageLevel.MEMORY_ONLY_SER()序列化仅内存
         * StorageLevel.MEMORY_AND_DISK()非序列化内存和磁盘
         * StorageLevel.MEMORY_AND_DISK_SER()序列化内存和磁盘
         * StorageLevel.DISK_ONLY();仅磁盘
         * 
         * 如果内存充足,可采用_2的双副本模式
         * StorageLevel.MEMORY_ONLY_2()
         */
        sessionid2actionRDD=sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());

        //下面开始第一步的聚合:首先按照session_ID进行groupByKey进行分组
        //此时的数据的粒度就是session粒度了,然后session粒度的数据与用户信息的数据进行join
        //然后就可以获得session粒度的数据,同时还包含了session对应的user的信息
        //通过以下方法,获得数据格式为<sessionid,fullAggrInfo(sessionid,searchKeyWords,clickCategoryIds,age,city,professional,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sessionid2actionRDD, sqlContext);

        
        //接着对聚合好的sessionid2AggrInfoRDD数据进行过滤
        //相当于我们编写的算子函数,是要访问外面的任务参数对象的
        //规则:匿名内部类(算子函数),访问外部对象,是要给外部对象使用final修饰的
        
        
            

            //重构,同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(//filteredSessionid2AggrInfoRDD之后也是使用了两次,因此也是要采用持久化机制
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        //持久化filteredSessionid2AggrInfoRDD
        filteredSessionid2AggrInfoRDD=filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        //生成公共的RDD:通过筛选条件的session的访问明细数据
        /**
         * 重构:sessionid2detailRDD,就是代表了通过筛选的session对应的访问明细数据
         */
        JavaPairRDD<String ,Row> sessionid2detailRDD=getSessionid2detailRDD(//sessionid2detailRDD被用了三次,也是要进行持久化
                filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
        //持久化
        sessionid2detailRDD=sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
       // System.out.println(filteredSessionid2AggrInfoRDD.count());
        /**
         * 特别说明,要将上一个功能的session获取到,必须要在出发action操作出发job之后,才能从accumulator中
         * 获取数据,否则时获取不到数据的,因为job没有执行,accumulator的值为空,
         * 所以我们将随机抽取的功能的实现代码,放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中,有一个countByKey算子,是action操作,会触发job
         */
        randomExtractSession(sc,task.getTaskId(),filteredSessionid2AggrInfoRDD,sessionid2actionRDD);

        //计算出各个session占比,然后写入MySQL(注意一定要进行action操作后才可以进行入库操作)
        //否则,transformation只是逻辑上读数据,而不是正式的处理数据,必须把action操作放在写入MySQL之前
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskId());


        /**
         * 获取top10热门商品,需要传入(过滤好的sessionid以及aggrInfo数据),和(sessionid以及row)
         */
        List<Tuple2<CategorySortKey,String>> top10CategoryList=getTop10Category(
                task.getTaskId(),sessionid2detailRDD);

        /**
         * 获取top10活跃session
         */
        getTop10Session(sc,task.getTaskId(),top10CategoryList,sessionid2detailRDD);
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

        return actionDF.javaRDD();//将从数据库查到的数据转成javaRDD,基本上转成了一个row的类型
    }

    /**
     *获取sessionid到访问行为数据的映射的RDD
     * @param actionRDD
     */
    public static JavaPairRDD<String,Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2),row);
            }
        });
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param  
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaPairRDD<String,Row> sessionid2actionRDD, SQLContext sqlContext) {
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

                        //session的起始结束时间
                        Date startTime = null;
                        Date endTime = null;
                        //session的访问步长
                        int stepLength = 0;


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

                            //计算session开始和结束时间,草泥马的蠢比教程写的有问题!!!!!!导致跑不起来,草泥马
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (endTime.after(actionTime)) {
                                endTime = actionTime;
                            }

                            //计算session访问步长
                            stepLength++;
                        }
                        //去掉末尾的","
                        String searchKeyWords = StringUtils.trimComma(searchkeywordBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //计算session访问时长(s)
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;


                        //因为我们要的是和用户表进行join,所以我们要把之前的<session_id,partAggrInfo>
                        //转化成<user_id,partAggrInfo>,还要再做一次maptopair算子(多此一举)
                        //因为可以直接定义返回的数据格式为<user_id,partAggrInfo>,然后和用户信息进行join
                        //将partAggrInfo关联上userInfo,再直接将返回的Tuple的key设置成sessionID
                        //最后的数据格式就是<session_id,partAggrInfo>

                        //聚合数据用什么样的格式拼接
                        //我们统一定义为key=value|key=value|......
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                                Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
                                Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                                Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                                Constants.FIELD_STEP_LENGTH + "=" + stepLength+"|"+
                                Constants.FIELD_START_TIME+"="+DateUtils.formatDate(startTime);

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
                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);


                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
                });
        return session2idFullInfoRDD;
    }//aggregateSession结束

    /**
     * 过滤已经聚合的session数据,并进行聚合统计
     *
     * @param sessionid2AggrInfoRDD
     * @return java
     */
    //当匿名内部类调用外部参数时,必须用final修饰
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam, final Accumulator<String> sessionAggrStatAccumulator) {
        //为了使用ValieUtils,我们首先将筛选参数拼成一个串,不要觉得多此一举,其实是为了性能优化埋下伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categotyIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categotyIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categotyIds + "|" : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //首先,从Tuple中获取聚合数据
                        String aggrInfo = tuple._2;
//                        System.out.println("aggrinfo----->"+aggrInfo);
                        //接着按照筛选条件进行筛选
                        //按照年龄范围进行过滤(采用ValidUtils工具类进行过滤)
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        //按照职业范围进行过滤,eg:互联网,IT,软件
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        //按照城市范围进行过滤eg:北京,上海,广东,深证
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        //按照性别进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        //按照搜索词过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类id过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        //如果经过了之前的多个过滤条件后,程序走到这边,说明该session是通过用户指定的筛选条件的,也就时需要保留的session
                        //所以就需要对session的访问时长和访问步长进行统计,根据session对应的范围,进行相应的累加计数
                        //只要走到这一步,就是需要进行计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        //计算出session访问时长和访问步长
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                       
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        System.out.println("访问时长和访问步长:"+visitLength+"---"+stepLength);
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);
                        return true;
                    }

                    /**
                     * 计算访问时长
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength >= 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength >= 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                });

        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     * @param sessionid2aggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String ,Row> getSessionid2detailRDD(
            JavaPairRDD<String,String> sessionid2aggrInfoRDD,
            JavaPairRDD<String,Row> sessionid2actionRDD){
        JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD.join(sessionid2actionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }
                });
        return sessionid2detailRDD;
    }
    
    
    
    
    /**
     * 随机抽取session
     * @param filteredSessionid2AggrInfoRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String ,Row> sessionid2actionRDD) {
        //第一步,计算每天每小时session的数量,获取<yyyy-MM-dd_HH,sessionid>格式的RDD
        JavaPairRDD<String,String > time2sessionidRDD=filteredSessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo=tuple._2;   
                String startTime=StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
                System.out.println("509:"+aggrInfo);
                String dateHour=DateUtils.getDateHour(startTime);//将yyyy-MM-dd HH:mm:ss--->yyyy-MM-dd_HH的日期格式
                
                
                return new Tuple2<String, String>(dateHour,aggrInfo);
            }
        });
        /**
         * 每天每小时的session数量,然后计算每天每小时的session抽取索引,遍历每天每小时session
         * 首先抽取的session的聚合数据,写入session_random_extract表
         * 所以第一个RDD的value,应该是session聚合数据
         */
        //得到每天每个小时的session数量<dateHour,count>
        Map<String, Object> countMap = time2sessionidRDD.countByKey();
        
        //第二步,按时间比例随机抽取算法,计算出每天每小时要抽取session索引
        //将<yyyy-MM-dd_HH,count>格式的map,转化成<yyyy-MM-dd,<HH,count>>格式
         Map<String ,Map<String ,Long>> dateHourCountMap=new HashMap<String, Map<String, Long>>();
        for(Map.Entry<String ,Object> countEntry:countMap.entrySet()){
            String dateHour=countEntry.getKey();
            String date=dateHour.split("_")[0];
            String hour=dateHour.split("_")[1];
            
            long count=Long.valueOf((Long) countEntry.getValue());
            
            //构造新的hashmap往dateHourCountMap里面塞
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap==null){
                hourCountMap=new HashMap<String, Long>();
                dateHourCountMap.put(date,hourCountMap);
            } 
            hourCountMap.put(hour,count);
        }
        //开始实现我们的按时间比例随机抽取算法
        //总共要抽取100个,先按照天数,进行平分
        long extractNumberPerDay=100/dateHourCountMap.size();//100/总共有的天数=一天要拉取的session数量
        
        //<date,<hour,(3,5,6,9)>>,因为将要在算子中使用dateHourExtractMap,所以用final修饰
        /**
         * session随机抽取功能
         * 用到了一个比较大的变量,随机抽取索引map,会比较消耗内存和网络带宽
         * 采用广播变量的方式
         * 
         */
       final Map<String, Map<String, ArrayList<Integer>>> dateHourExtractMap=
                new HashMap<String, Map<String, ArrayList<Integer>>>();
        Random random=new Random();
        
        //遍历dateHourCountMap:<date,<hour,count>> 
        for(Map.Entry<String,Map<String ,Long>> dateHourCountEntry:dateHourCountMap.entrySet()){
            String date=dateHourCountEntry.getKey();//获取日期
            Map<String, Long> hourCountMap=dateHourCountEntry.getValue();
            
            //计算这一天的session总数
            long sessionCount=0L;
            for (long  hourCount:hourCountMap.values()){
                sessionCount+=hourCount;
            }
            
            //提出来<hour,List<Integer>(3,5,6,9)>
            Map<String, ArrayList<Integer>> hourExtractMap=dateHourExtractMap.get(date);
            if(hourExtractMap==null){
                hourExtractMap= new HashMap<String, ArrayList<Integer>>();
                dateHourExtractMap.put(date,hourExtractMap);
            }
            
            //遍历每个小时
            for(Map.Entry<String ,Long> hourCountEntry:hourCountMap.entrySet() ){
                String hour=hourCountEntry.getKey();
                long count=hourCountEntry.getValue();
                
                //计算每个小时的session数量,占据当天总session数量的比例,直接乘以每天要抽取的量
                //就可以计算出当前小时要抽取的session数量
                int hourExtractNumber= (int) (((double)count/(double)sessionCount)*extractNumberPerDay);
                
                if (hourExtractNumber>count){//当前要取的session数量大于count,直接取完所有,即count的大小
                    hourExtractNumber=(int)count;
                }
                //先获取当前小时的存放随机数的list
                ArrayList<Integer> extractIndexList=hourExtractMap.get(hour);
                if(extractIndexList==null){
                    extractIndexList= new ArrayList<Integer>();
                    hourExtractMap.put(hour,extractIndexList);
                }
                
                
                //生成上面计算出来的数量的随机数
                for(int i=0;i<hourExtractNumber;i++){
                    int extractIndex=random.nextInt((int)count);
                    while (extractIndexList.contains(extractIndex)){
                        extractIndex=random.nextInt((int)count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
            
        }

        //将变量广播化
        final Broadcast<Map<String, Map<String, ArrayList<Integer>>>> dateHourExtractMapBroadcast=
                sc.broadcast(dateHourExtractMap);

        /**
         * 第三步,遍历每小时的session,然后根据随机索引进行抽取
         */
        //执行groupByKey算子,得到<datehour,<session,aggrInfo>>
        JavaPairRDD<String ,Iterable<String >> time2sessionsRDD=time2sessionidRDD.groupByKey();
        
        //我们利用flatmap算子,遍历所有<datehour,<session,aggrInfo>>格式的数据
        //然后遍历每天每小时的session,如果发现某个session在我们指定的索引上
        //那么抽取该session,直接写入MySQL中的random_extract_session表
        //将抽取出来的session id返回回来,形成一个新的JavaRDD<String>
        //最后一步,用抽取出来的sessionid,去join他们的访问行为明细数据,写入session_detail表中
        JavaPairRDD<String,String > extractSessionidsRDD=time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String , String >() {
                    @Override
                    public Iterable<Tuple2<String , String >> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                        ArrayList<Tuple2<String ,String >> extractSessionids=new ArrayList<Tuple2<String, String>>();

                        String dateHour=tuple._1;
                        String date=dateHour.split("_")[0];
                        String hour=dateHour.split("_")[1];
                        Iterator<String> iterator= tuple._2.iterator();

                        //所有的session索引
                        /**
                         * 使用广播变量的时候,直接调用广播变量(broadcast类型)的value/getValue方法就好了
                         * 这样就可以获得之前封装的广播变量了
                         */
                        Map<String, Map<String, ArrayList<Integer>>> dateHourExtractMap=dateHourExtractMapBroadcast.value();
                        ArrayList<Integer> extractIndexList=dateHourExtractMap.get(date).get(hour);//dateHourExtractMap中是<date,<hour,(3,5,6,9)>>样的格式

                        ISessionRandomExtractDAO sessionRandomExtractDAO=DAOFactory.getSessionRandomExtractDAO();
                        int index=0;
                        while(iterator.hasNext()){
                            String sessionAggrInfo=iterator.next();

                            //一旦符合条件的session,就
                            if(extractIndexList.contains(index)){
                                String sessionid=StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                                //将符合条件的session插入MySQL
                                SessionRandomExtract sessionRandomExtractDomain=new SessionRandomExtract();
                                sessionRandomExtractDomain.setTaskid(taskid);
                                sessionRandomExtractDomain.setSessionid(sessionid);
                                sessionRandomExtractDomain.setStartTime(
                                        StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_START_TIME));
                                sessionRandomExtractDomain.setSearchKeywords(
                                        StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtractDomain.setClickCategoryIds(
                                        StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtractDomain);
                                //将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid,sessionid));
                            }
                            index++;
                        }
                        return extractSessionids;
                    }
                });


        /**
         * 第四部:获取抽取出来的明细数据
         */
        //得到的结果是一行:<sessionid,<page_id,row>>的数据
        JavaPairRDD<String, Tuple2<String, Row>> extractDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD);
        extractDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row=tuple._2._2;
                SessionDetail sessionDetail=new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                //调用factory插入session_detail表
                ISessionDetailDAO sessionDetailDAO=DAOFactory.getSessionDetilDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }

    /**
     * 计算哥session范围占比,并写入MySQL
     *
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value,long taskid) throws SQLException {
        long session_count=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.SESSION_COUNT));
        long visit_length_1s_3s=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.TIME_PERIOD_30m));
        
        long step_length_1_3=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.STEP_PERIOD_1_3));
        long step_length_4_6=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.STEP_PERIOD_4_6));
        long step_length_7_9=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.STEP_PERIOD_7_9));
        long step_length_10_30=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.STEP_PERIOD_10_30));
        long step_length_30_60=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.STEP_PERIOD_30_60));
        long step_length_60=Long.valueOf(StringUtils.getFieldFromConcatString(
                value,"\\|",Constants.STEP_PERIOD_60));
        //要提前转化成都变了类型,不然会导致结果为零
        double visit_length_1s_3s_ratio= NumberUtils.formatDouble((double)visit_length_1s_3s/(double)session_count,2);
        double visit_length_4s_6s_ratio= NumberUtils.formatDouble((double)visit_length_4s_6s/(double)session_count,2);
        double visit_length_7s_9s_ratio= NumberUtils.formatDouble((double)visit_length_7s_9s/(double)session_count,2);
        double visit_length_10s_30s_ratio= NumberUtils.formatDouble((double)visit_length_10s_30s/(double)session_count,2);
        double visit_length_30s_60s_ratio= NumberUtils.formatDouble((double)visit_length_30s_60s/(double)session_count,2);
        double visit_length_1m_3m_ratio= NumberUtils.formatDouble((double)visit_length_1m_3m/(double)session_count,2);
        double visit_length_3m_10m_ratio= NumberUtils.formatDouble((double)visit_length_3m_10m/(double)session_count,2);
        double visit_length_10m_30m_ratio= NumberUtils.formatDouble((double)visit_length_10m_30m/(double)session_count,2);
        double visit_length_30m_ratio= NumberUtils.formatDouble((double)visit_length_30m/(double)session_count,2);
        
        double step_length_1_3_ratio=NumberUtils.formatDouble((double)step_length_1_3/(double)session_count,2);
        double step_length_4_6_ratio=NumberUtils.formatDouble((double)step_length_4_6/(double)session_count,2);
        double step_length_7_9_ratio=NumberUtils.formatDouble((double)step_length_7_9/(double)session_count,2);
        double step_length_10_30_ratio=NumberUtils.formatDouble((double)step_length_10_30/(double)session_count,2);
        double step_length_30_60_ratio=NumberUtils.formatDouble((double)step_length_30_60/(double)session_count,2);
        double step_length_60_ratio=NumberUtils.formatDouble((double)step_length_60/(double)session_count,2);
        
        //将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat=new SessionAggrStat();
        
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
        
        //调用对应的dao插入统计的结果
        ISessionAggrStatDAO sessionAggrStatDAO=DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


    /**
     * 获取top10热门商品
     * @param taskid
     * @param sessionid2detailRDD
     */
    private static List<Tuple2<CategorySortKey,String >> getTop10Category(
            final long taskid,
            JavaPairRDD<String ,Row> sessionid2detailRDD) throws SQLException {
        //第一步,获取符合条件的session的访问明细(将filteredSessionid2AggrInfoRDD和sessionid2actionRDD join起来)
        //首先得到<sessionid,aggrInfo,row>的数据格式,然后进行map过程,得到<string,row>的数据类型
       
        
        
        

        //获取session访问过的所有品类id,访问过指的是点击过,下单过,支付过的品类,所以要用flatMaptoPair算子
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;//拿到每个sessionid对应的明细
                ArrayList<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                Long categoryId = row.getLong(6);
                if (categoryId != null) {
                    list.add(new Tuple2<Long, Long>(categoryId, categoryId));
                }

                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null) {
                    String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                    for (String orderCategoryId : orderCategoryIdsSplited) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                    }
                }

                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    String[] payCategoryIdsSplited = payCategoryIds.split(",");
                    for (String payCategoryId : payCategoryIdsSplited) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                    }
                }

                return list;
            }
        });

        /**
         * 必须要进行去重
         * 如果不去重的话,会出现重复的categoryid,排序会对重复的categoryid以及countInfo进行排序
         * 最后很可能会拿到重复的数据
         */
        categoryidRDD=categoryidRDD.distinct();
        
        //第二步:计算品类点击,下单,支付次数,三种访问行为是点击,下单和支付,分别计算各品类的点击,下单和支付次数
        //可以先对访问明细进行过滤,过滤完成后,然后通过map,reduceByKey等算子进行计算
            //统计点击次数
       JavaPairRDD<Long,Long> clickCategoryId2CountRDD= getClickCategoryId2CountRDD(sessionid2detailRDD);
            //统计下单次数
        JavaPairRDD<Long, Long> orderCategaryId2CountRDD = getOrderCategaryId2CountRDD(sessionid2detailRDD);
        //统计支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

        /**
         * 第三步:join各品类与它的点击,下单,和支付次数
         * categoryRDD中,包含了所有符合条件的session,访问过的品类id
         * 上面计算出来的三份,分别是点击,下单,支付次数,可能不是包含所有品类的(因为有的品类只是被点击过,没有被下单和支付过)
         * 对于此,不能使用join操作(没有join上的就不会展现结果),要采用leftjoin操作(因为还要保留categoryId数据)
         */
        JavaPairRDD<Long,String> categoryid2CountRDD=joinCategoryAndDate(
                categoryidRDD,clickCategoryId2CountRDD,orderCategaryId2CountRDD,payCategoryId2CountRDD);

        /**
         * 第四步:自定义二次排序key
         */

        /**
         * 第五步:将数据映射成<categorySortKey(自己定义的排序规则),info(clickCount=...|orderCount=...|payCount=...)>格式,然后进行二次排序
         */
        JavaPairRDD<CategorySortKey,String> sortKey2countRDD=categoryid2CountRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                String countInfo=tuple._2;//countInfo中是clickCount=...|orderCount=...|payCount=...
                long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
                long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|", Constants.FIELD_ORDER_COUNT));
                long payCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo ,"\\|",Constants.FIELD_PAY_COUNT));
                CategorySortKey categorySortKey=new CategorySortKey(clickCount,orderCount,payCount);
                return new Tuple2<CategorySortKey, String>(categorySortKey,countInfo);
            }
        });
        JavaPairRDD<CategorySortKey,String> sortedCategoryCountRDD=sortKey2countRDD.sortByKey(false);//false的含义是降序排列,虽然采用的是sortByKey,但在sortKey2countRDD中已经自定义好排序规则,所以默认按照自定义的规则来
        
        /**
         * 第六步:采用take(10)取出top10热门品类,并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO=DAOFactory.getTop10CategoryDAP();
        Top10Category top10Category=new Top10Category();

        List<Tuple2<CategorySortKey, String>> top10CategoryList=
                 sortedCategoryCountRDD.take(10);
        for(Tuple2<CategorySortKey,String> tuple:top10CategoryList){
            String countInfo=tuple._2;
            long categoryid=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID));
            long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
            long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT));
            long payCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT));

            top10Category.setTaskid(taskid);
            top10Category.setCategoryid(categoryid);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            top10CategoryDAO.insert(top10Category);
        }
    return top10CategoryList;
    }//getTop10Category方法结束       
       
       
        //由于上面三个方法其中分为三部的transformation,可以将着三个步骤封装在一个方法里
        /**
         * 获取各个品类点击次数
         */
        private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(JavaPairRDD<String,Row> sessionid2detailRDD){
            //点击
            JavaPairRDD<String,Row> clickActionRDD=sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Row> tuple)  throws Exception {
                    Row row=tuple._2;
                    return row.get(6)!=null?true:false;
                }
            });

            //统计点击次数的算子,用mapToPair算子,简称拆1
            JavaPairRDD<Long,Long> clickCategoryIdRDD=clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
                @Override
                public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                    long clickCategory=tuple._2.getLong(6);
                    return new Tuple2<Long, Long>(clickCategory,1L);
                }
            });

            //聚合拆1,采用reduceByKey算子
            JavaPairRDD<Long,Long> clickCategoryId2CountRDD=clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long v1, Long v2) throws Exception {
                    return v1+v2;
                }
            });
            
            return clickCategoryId2CountRDD;
        }//是getClickCategoryidsCountRDD方法结束

    /**
     * 获取各类商品订单次数
     * @param sessionid2detailRDD
     * @return
     */
        private static JavaPairRDD<Long,Long> getOrderCategaryId2CountRDD(JavaPairRDD<String,Row> sessionid2detailRDD){
            //计算各个品类的下单次数
            JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                    Row row = tuple._2;
                    return Long.valueOf(row.getLong(8)) != null ? true : false;
                }
            });
            JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                    new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                        @Override
                        public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                            Row row = tuple._2;
                            String orderCategoryIds = row.getString(8);
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                            ArrayList<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                            }
                            return list;
                        }
                    });
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long v1, Long v2) throws Exception {
                    return v1 + v2;
                }
            });
            
            return orderCategoryId2CountRDD;
        }

    /**
     * 获取各类商品支付次数
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getPayCategoryId2CountRDD(JavaPairRDD<String,Row> sessionid2detailRDD){
            //计算各个品类的支付次数
            JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                    Row row = tuple._2;
                    return Long.valueOf(row.getLong(10)) != null ? true : false;
                }
            });
            JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                    new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                        @Override
                        public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                            Row row = tuple._2;
                            String payCategoryIds = row.getString(10);
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            ArrayList<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                            }
                            return list;
                        }
                    });
            JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long v1, Long v2) throws Exception {
                    return v1 + v2;
                }
            });
            
            return payCategoryId2CountRDD;
        }

    /**
     * 左外连接品类RDD和数据RDD
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long,String> joinCategoryAndDate(
            JavaPairRDD<Long,Long> categoryidRDD,
            final JavaPairRDD<Long,Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long,Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long,Long> payCategoryId2CountRDD){
        //这里的com.google.common.base.Optional是表示有可能为空,因为leftouterJoin时右边是有可能为空的
        JavaPairRDD<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        //转化成<Long,String>格式,leftOutJoin点击次数
        JavaPairRDD<Long,String> tmpMapRDD=tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> tuple) throws Exception {
                long categoryid=tuple._1;
                com.google.common.base.Optional<Long> clickCategoryOptional=tuple._2._2;
                long clickCount=0L;
                
                if(clickCategoryOptional.isPresent()){
                    clickCount=clickCategoryOptional.get();
                }
                
                String value=Constants.FIELD_CATEGORY_ID+"="+categoryid+Constants.FIELD_CLICK_COUNT+"="+clickCount;
                return new Tuple2<Long,String>(categoryid,value);
            }
        });
    
        //leftOutJoin下单次数,获得时<categoryid,value,次数>的三个元素的格式
        tmpMapRDD=tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>> tuple) throws Exception {
                long categoryid=tuple._1;//拿到categoryid
                String value=tuple._2._1;//拿到value
                com.google.common.base.Optional<Long> orderCategoryOptional=tuple._2._2;
                long orderCount=0L;
                
                if(orderCategoryOptional.isPresent()){
                    orderCount=orderCategoryOptional.get();
                }
                value=value+"|"+Constants.FIELD_ORDER_COUNT+"="+orderCount;
                
                return new Tuple2<Long, String>(categoryid,value);
            }
        });
        
        //leftOutJoin支付次数
        tmpMapRDD=tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>> tuple) throws Exception {
                long categoryid=tuple._1;
                String value=tuple._2._1;
                com.google.common.base.Optional<Long> payCategoryOptional=tuple._2._2;
                long payCount=0L;
                if(payCategoryOptional.isPresent()){
                    payCount=payCategoryOptional.get();
                }
                
                value=value+"|"+Constants.FIELD_PAY_COUNT+"="+payCount;
                return new Tuple2<Long, String>(categoryid,value);
            }
        });
        return tmpMapRDD;   
    }

    /**
     * 获取top10活跃session方法
     * @param taskid
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskid, 
            List<Tuple2<CategorySortKey,String>> top10CategoryList,
            JavaPairRDD<String,Row> sessionid2detailRDD) {
        /**
         * 第一步:将top10热门品类的id,生成一份RDD
         */
        final List<Tuple2<Long,Long>> top10CategoryIdList=new ArrayList<Tuple2<Long, Long>>();
        
        for(Tuple2<CategorySortKey,String> category:top10CategoryList){
            long categoryid=Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2,"\\|",Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid,categoryid));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步:计算top10品类被个session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();
        JavaPairRDD<Long,String> categoryid2sessionCountRDD=sessionid2detailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionid=tuple._1;
                Iterator<Row> iterator=tuple._2.iterator();

                Map<Long,Long> categoryCountMap=new HashMap<Long, Long>();
                //计算出该session对每个品类对应的点击次数
                while(iterator.hasNext()){
                    Row row=iterator.next();
                    if(row.get(6)!=null){
                        long categoryid=row.getLong(6);
                        Long count=categoryCountMap.get(categoryid);
                        if(count==null){
                            count=0L;
                        }
                        count++;
                        categoryCountMap.put(categoryid,count);
                    }
                }
                //返回结果,<categoryid,sessionid,count>格式
                List<Tuple2<Long,String>> list=new ArrayList<Tuple2<Long, String>>();
                for(Map.Entry<Long,Long> categoryEntry:categoryCountMap.entrySet()){
                    long categoryid=categoryEntry.getKey();
                    long count=categoryEntry.getValue();
                    String value=sessionid+","+count;

                    list.add(new Tuple2<Long, String>(categoryid,value));
                }
                return list;
            }
        });
        //获取top10热门品类,被各个session点击的次数
       JavaPairRDD<Long,String> top10CategorySessionCountRDD= 
               top10CategoryIdRDD.join(categoryid2sessionCountRDD)//<categoryIDRDD(top10)>join<categoryid,sessionCount(统计好的session)>
               .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                   @Override
                   public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                       return new Tuple2<Long, String>(tuple._1,tuple._2._2);//返回的是
                   }
               });
        /**
         * 第三步:分组取TopN算法实现,获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();
        JavaPairRDD<String,String> top10SessionRDD=top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String , String >() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                long categoryid=tuple._1;//获取categoryid
                Iterator<String> iterator=tuple._2.iterator();
                String[] top10Sessions=new String[10];
                
                while(iterator.hasNext()){
                    String sessionCount=iterator.next();
                    String sessionid=sessionCount.split(",")[0];
                    long count=Long.valueOf(sessionCount.split(",")[1]);
                    
                    //用于获取前十名放在数组中
                    for(int i=0;i<top10Sessions.length;i++){
                        if(top10Sessions[i]==null){//因为topSessions已经是排好序的
                            top10Sessions[i]=sessionCount;
                            break;
                        }else {//遇到不为空的sessionCount的时候,进行挨个的比较
                            long _count=Long.valueOf(top10Sessions[i].split(",")[1]);
                            if(count>_count){//当老的比新来的大的时候,开始将数组向后挪一个
                                for(int j=9;i>=0;j--){
                                    top10Sessions[j]=top10Sessions[j-1];
                                }
                                top10Sessions[i]=sessionCount;//因为原来的top10Session[i]位置已经挪走,所以将top10Session[i]重新赋值
                                break;
                            }
                        }
                    }
                }
                //插入Mysql数据库
                List<Tuple2<String,String>> list=new ArrayList<Tuple2<String, String>>();//用来呈装返回的tuple2<sessionid,sessionid>
                ITop10SesssionDAO top10SesssionDAO=DAOFactory.getTop10SessionDAO();
                for(String sessionCount:top10Sessions){
                    String sessionid=sessionCount.split(",")[0];//从前十的表中去除sessionid
                    long count=Long.valueOf(sessionCount.split(",")[1]);//从前十的表中取出统计的内容
                    
                    Top10Session top10Session=new Top10Session();//创建domain对象
                    top10Session.setTaskid(taskid);
                    top10Session.setCategoryid(categoryid);
                    top10Session.setSessionid(sessionid);
                    top10Session.setClickCount(count);
                        //插入数据库
                    top10SesssionDAO.insert(top10Session);
                    
                    list.add(new Tuple2<String, String>(sessionid,sessionid));
                }
                return list;
            }
        });//第三步结束

        /**
         * 第四步:获取top10活跃session的明细数据,并写入Mysql
         */
            //将top10Session和sessionid2detailRDDjoin起来,得到,<sessionid,sessionid,Row>       
        JavaPairRDD<String,Tuple2<String,Row>> sessionDetailRDD=top10SessionRDD.join(sessionid2detailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row=tuple._2._2;
                SessionDetail sessionDetail=new SessionDetail();

                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetilDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
        
        
    }
}


    
    
    
    
    
    
    
    
    
    
    
    

