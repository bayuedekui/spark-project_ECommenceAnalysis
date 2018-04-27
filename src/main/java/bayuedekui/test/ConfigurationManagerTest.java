package bayuedekui.test;

import bayuedekui.sparkproject.conf.ConfigurationManager;

/**
 * 测试管理组件测试类
 */
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String testkey1= ConfigurationManager.getProperty("mytest1");
        String testkey2= ConfigurationManager.getProperty("mytest2");
        System.out.println(testkey1+" "+testkey2 );
    }
}
