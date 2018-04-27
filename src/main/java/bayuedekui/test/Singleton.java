package bayuedekui.test;

/**
 * 单例模式
 * 
 * 
 * 
 */
public class Singleton {
    private static Singleton instance=null;
    
    private Singleton(){
        
    }
    public static Singleton getInstance(){
        if(instance==null){
            synchronized(Singleton.class){//防止多个线程同时构造单例
                if(instance==null){
                    instance=new Singleton();
                }
            }
        }
        return instance;
    }
}
