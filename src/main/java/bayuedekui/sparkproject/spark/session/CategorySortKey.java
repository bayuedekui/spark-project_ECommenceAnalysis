package bayuedekui.sparkproject.spark.session;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 品类二次排序可key
 * 封装你需要进行比较的几个字段:点击次数,下单次数,和支付次数
 * 实现Ordered接口的几个方法
 * 和其他key相比,如何来判定大于,大于等于,小于,小于等于
 * 依次进行三个字段进行相比,如果等于就进行下一个相比
 * (自定义二次排序key,必须要实现序列化接口,表明可以是可以序列化的,否则会报错)
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private long clickCount;
    private long orderCount;
    private long payCount;
    
    @Override
    public int compare(CategorySortKey other) {
       if(clickCount-other.getClickCount()!=0){
           return (int) (clickCount-other.getClickCount());
       }else if(orderCount-other.getOrderCount()!=0){
           return (int) (orderCount-other.getOrderCount());
       }else if(payCount-other.getPayCount()!=0){
           return (int) (payCount-other.getPayCount());
       }
       return 0;
    }

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        if(clickCount-other.getClickCount()!=0){
            return (int) (clickCount-other.getClickCount());
        }else if(orderCount-other.getOrderCount()!=0){
            return (int) (orderCount-other.getOrderCount());
        }else if(payCount-other.getPayCount()!=0){
            return (int) (payCount-other.getPayCount());
        }
        return 0;
        

    }
    @Override
    public boolean $less(CategorySortKey other) {
        if(clickCount<other.getClickCount()){
            return true;
        }else if(clickCount==other.getClickCount()&&orderCount<other.getOrderCount()){
            return true;
        }else if(orderCount==other.getOrderCount()&&payCount<other.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
       if(clickCount>other.getClickCount()){
           return true;
       }else if(clickCount==other.getClickCount()&&orderCount>other.getOrderCount()){
           return true;
       }else if(orderCount==other.getOrderCount()&&payCount>other.getPayCount()){
           return true;
       }
       return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if($less(other)){
            return true;
        }else if(clickCount==other.getClickCount()&&orderCount==other.getOrderCount()&&payCount==other.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
       if($greater(other)){
           return true;
       }else if(clickCount==other.getClickCount()&&orderCount==other.getOrderCount()&&payCount==other.getPayCount()){
           return true;
       }
       return false;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    
    
    
}
