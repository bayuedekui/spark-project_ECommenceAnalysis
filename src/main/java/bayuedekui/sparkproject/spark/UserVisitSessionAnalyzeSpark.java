package bayuedekui.sparkproject.spark;

import bayuedekui.sparkproject.conf.ConfigurationManager;
import bayuedekui.sparkproject.constant.Constants;
import bayuedekui.sparkproject.dao.ISessionAggrStatDAO;
import bayuedekui.sparkproject.dao.ITaskDao;
import bayuedekui.sparkproject.dao.impl.DAOFactory;
import bayuedekui.sparkproject.domain.SessionAggrStat;
import bayuedekui.sparkproject.domain.Task;
import bayuedekui.sparkproject.util.*;
import bayuedekui.test.MockData;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.bcel.internal.classfile.ConstantString;
import org.apache.calcite.util.NumberUtil;
import org.apache.spark.Accumulator;
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
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.apache.spark.sql.columnar.LONG;
import org.apache.spark.sql.hive.HiveContext;
import py4j.StringUtil;
import scala.Tuple2;
import scala.tools.nsc.doc.DocFactory;

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
//        System.out.println("row-->"+task.getTaskName()+task.getTaskParam());
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

        
        //接着对聚合好的sessionid2AggrInfoRDD数据进行过滤
        //相当于我们编写的算子函数,是要访问外面的任务参数对象的
        //规则:匿名内部类(算子函数),访问外部对象,是要给外部对象使用final修饰的
        
        
        

        //重构,同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);


        System.out.println(filteredSessionid2AggrInfoRDD.count());
        /**
         * 特别说明,要将上一个功能的session获取到,必须要在出发action操作出发job之后,才能从accumulator中
         * 获取数据,否则时获取不到数据的,因为job没有执行,accumulator的值为空,
         * 所以我们将随机抽取的功能的实现代码,放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中,有一个countByKey算子,是action操作,会触发job
         */
        randomExtractSession(filteredSessionid2AggrInfoRDD);

        //计算出各个session占比,然后写入MySQL(注意一定要进行action操作后才可以进行入库操作)
        //否则,transformation只是逻辑上读数据,而不是正式的处理数据,必须把action操作放在写入MySQL之前
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskId());


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

                            //计算session开始和结束时间
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
                        String partAggrInfo = Constants.FILED_SESSION_ID + "=" + sessionId + "|" +
                                Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
                                Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                                Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                                Constants.FIELD_STEP_LENGTH + "=" + stepLength+"|"+
                                Constants.FIELD_START_TIME+DateUtils.formatDate(startTime);

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
                        System.out.println("aggrinfo----->"+aggrInfo);
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
     * 随机抽取session
     * @param filteredSessionid2AggrInfoRDD
     */
    private static void randomExtractSession(JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD) {
        //计算每天每小时session的数量,获取<yyyy-MM-dd_HH,sessionid>格式的RDD
        JavaPairRDD<String,String > time2sessionidRDD=filteredSessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo=tuple._2;   
                String startTime=StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
                String dateHour=DateUtils.getDateHour(startTime);//将yyyy-MM-dd HH:mm:ss--->yyyy-MM-dd_HH的日期格式
                
                
                return new Tuple2<String, String>(dateHour,aggrInfo);
            }
        });
        /**
         * 每天每小时的session数量,然后计算每天每小时的session抽取索引,遍历每天每小时session
         * 首先抽取的session的聚合数据,写入session_random_extract表
         * 所以第一个RDD的value,应该是session聚合数据
         */
        //第一步,得到每天每个小时的session数量<dateHour,count>
        Map<String, Object> countMap = time2sessionidRDD.countByKey();
        
        //第二部,按时间比例随机抽取算法,计算出每天每小时要抽取session索引
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
        
        //<date,<hour,(3,5,6,9)>>
        Map<String ,Map<String ,List<Integer>>> dateHourExtractMap=
                new HashMap<String, Map<String, List<Integer>>>();
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
            Map<String ,List<Integer>> hourExtractMap=dateHourExtractMap.get(date);
            if(hourExtractMap==null){
                hourExtractMap=new HashMap<String, List<Integer>>();
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
                List<Integer> extractIndexList=hourExtractMap.get(hour);
                if(extractIndexList==null){
                    extractIndexList=new ArrayList<Integer>();
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

    
}


    
    
    
    
    
    
    
    
    
    
    
    

