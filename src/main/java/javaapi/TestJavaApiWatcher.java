package javaapi;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 使用原生方式实现zookeeper增删改查
 */
public class TestJavaApiWatcher implements Watcher {
    private static final String SERVER = "192.168.128.148:2181";
    private static final int SESSION_TIMEOUT = 30000;

    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private ZooKeeper zk = null;



    /**
     * 建立连接
     * @param connectionString
     * @param sessionTimeout
     */
    public void createConnection(String connectionString,int sessionTimeout){
        this.releaseConnection();
        try {
            zk = new ZooKeeper(connectionString,sessionTimeout,this);
            countDownLatch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public void releaseConnection(){
        if(null != this.zk){
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建节点
     * @param path
     * @param data
     * @return
     */
    public boolean createPath(String path,String data){
        try {
            this.zk.exists(path,true);
            System.out.println("创建节点成功，path:"
                    + this.zk.create(path,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL)+",content:"+data);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 读取数据
     * @param path
     * @return
     */
    public String readDate(String path,boolean watch){
        try {
            return new String(this.zk.getData(path,watch,null));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 修改数据
     * @param
     */
    public boolean writeData(String path,String data){
        try {

            this.zk.setData(path,data.getBytes(),-1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除数据
     * @param path
     */
    public void deleteNode(String path){
        try {
            this.zk.delete(path,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        String path  = "/useJava";
         TestJavaApiWatcher api = new TestJavaApiWatcher();
         api.createConnection(SERVER,SESSION_TIMEOUT);
        if(api.createPath(path,"使用java原生api操作你")){
            System.out.println("数据内容"+api.readDate(path,true));
//            api.zk.exists(path,true);
            api.writeData(path,"使用java原生api操作你(改)");
            System.out.println("数据修改后"+api.readDate(path,true));
//            api.zk.exists(path,true);
            api.deleteNode(path);
        }
        api.releaseConnection();
    }

    /**
     *
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
//        System.out.println("收到事件通知:"+event.getState()+"\n");
          Event.EventType type = event.getType();
        if(Event.KeeperState.SyncConnected==event.getState()){
            countDownLatch.countDown();
            if(Event.EventType.NodeCreated  == type){
                System.out.println("创建了。。。。");
            }else if(Event.EventType.NodeDataChanged  == type){
                System.out.println("修改了。。。。");
            }else if(Event.EventType.NodeDeleted==type){
                System.out.println("删除了。。。。");
            }
        }
    }
}
