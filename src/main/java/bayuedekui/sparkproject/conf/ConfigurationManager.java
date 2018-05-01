package bayuedekui.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *  1.配置管理组件可以很复杂,也可以很简单,对于简单的配置管理组件,只需要开发一个类,可以在第一次访问中
 *      从对应的properties文件中,读取配置项,并提供外界获取配置key对应的value方法
 *  2.复杂的配置管理组件:可能需要用到设计模式,例如单例,工厂,适配器模式,可能需要多个不同的properties,甚至是xml文件类型的配置文件
 *  3.我们在这里使用的是简单地配置
 */
public class ConfigurationManager {
    /**
     *prop之所以写成私有,是为了防止外界通过Configuration对象直接拿到Properties对象,错误的更新了Proerties中某个
     * key对应的value,从而导致程序崩溃
     */
     private static Properties prop=new Properties();
    /**
     * 静态代码块
     * 
     * java中,每个类第一次使用过程中,就会被java虚拟机(JVM)中的类加载器加载,去从磁盘上的.class文件中加载出来
     * 然后为每个类创建一个class对象,就代表了这个类
     * 
     * 每个类在第一次加载的过程中,都会进行自身的初始化,由每个类内部的static{}构成的静态代码决定,我们自身可以在
     * 类中开发静态代码块,类第一次使用的时候,就会加载,就会初始化类,初始化类的时候执行类的静态代码块
     * 
     * 因此我们的配置管理组件,可以写在静态代码块中,如此,第一次外界代码调用这个ConfigurationManager类的静态方法的时候
     * 就会加载配置文件中的数据
     * 
     * 而且,在静态代码块中,累的初始化在JVM生命中,有且仅有一次,即配置文件只会被加载一次,以后重复使用
     */
    static {
        try {
            //通过类名.class的方式(反射),就可以获得这个类在jvm中对应的class对象
            //再通过这个Class对象的getClassLoad()方法获得当初加载这个类的类加载器(ClassLoader),
            //然后调用类加载器的getResourceAsStream()方法
            //就可以用类加载器,去加载路径中的指定文件
            //最终可以获得到一个,针对指定文件的输入流(InputStream)
            InputStream in=ConfigurationManager.class
                    .getClassLoader().getResourceAsStream("my.properties");
            //调用Properties的load方法,给他传入一个InputStream输入流
            //即可将文件中的符合"key-value"格式的配置项,加载到Properties对象中
            //加载过后,此时,Properties对象中就有了配置文件中所有的key-value对了
            //外界就可以通过Properties对象获取指定的key对应的value了
            prop.load(in);
        }catch (Exception e){
            e.printStackTrace();    
        }
        
    }

    /**
     * 获取指定key对应的value
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项,获取一个构造器对象,利用构造器的getReasourceAsStream()
     * 方法读取my.properties配置文件,通过Properties对象的getPoperty(key),获取my.peorperties文件
     * 的value值
     * 
     * 
     */
    public static Integer getIntger(String key){
        String value=getProperty(key);
        try {
            return Integer.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key){
        String value=getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return false;
    }
    
    public static Long getLong(String key){
        String value=getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return 0L;
    }
}
