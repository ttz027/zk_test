package curator;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorTestDemo1 {
    private static final String SERVER = "192.168.128.149:2181";
    private static final int SESSION_TIMEOUT = 30000;

    private static final int CONNECTION_TIMEOUT = 5000;

    private CuratorFramework client = null;

    /**
     * 重试时间和次数
     */
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
    @Before
    public void init(){
//        创建 实例
        client = CuratorFrameworkFactory.newClient(SERVER,SESSION_TIMEOUT,CONNECTION_TIMEOUT,retryPolicy);
//        启动
        client.start();
    }

    /**
     * 创建各种类型的节点
     * @throws Exception
     */
    @Test
    public void useCurator() throws Exception {
        //创建永久节点
        client.create().forPath("/curator","coratorTemplate".getBytes());
        //创建永久有序节点
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/curator_seq","coratorTemplate_SEQ".getBytes());
        //创建临时节点
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/ls_curator","coratorTssemplate".getBytes());
        //创建临时有序节点
        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/ls_curator_seq","coratossrTemplate_SEQ".getBytes());
    }

    /**
     * 操作节点
     * @throws Exception
     */
    @Test
    public void useCuratorDoNode() throws Exception {
        //修改
        client.setData().forPath("/curator","update".getBytes());
        //读取
        String getData =  new String(client.getData().forPath("/curator"));
        System.out.println(getData);
//        删除
        client.delete().forPath("/curator");
    }

    /**
     * 查看节点是否存在
     * @throws Exception
     */
    @Test
    public void nodeCheck() throws Exception{
         Stat stat = client.checkExists().forPath("/first");
        System.out.println("first 节点是否存在"+stat!=null?true:false);
        System.out.println(stat);
    }

    @Test
    public void testSEDataAsyc() throws Exception {
        CuratorListener listener = new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println(event);
            }
        };

        //添加监听器
        client.getCuratorListenable().addListener(listener);
        //异步修改节点数据
        client.setData().forPath("/curator","sync )".getBytes());

        Thread.sleep(10000);
    }

    /**
     * 简单的监听,此监听只能监听一次
     */
    @Test
    public void TestListenterOne() throws Exception{
        client.create().orSetData().withMode(CreateMode.PERSISTENT).forPath("/test","test".getBytes());
        byte[] bytes = client.getData().usingWatcher(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("获取test 节点监听"+event);
            }
        }).forPath("/test");
        client.create().orSetData().withMode(CreateMode.PERSISTENT).forPath("/test","test".getBytes());
        client.create().orSetData().withMode(CreateMode.PERSISTENT).forPath("/test","test".getBytes());
        System.out.println("节点数据："+new String(bytes));
        Thread.sleep(10000);
    }

    /**
     * path cache:监听子节点的变化
     * @throws Exception
     */
    @Test
    public void bestListenterPathCache() throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/test", true);
        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (data != null) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            System.out.println("create ==== " + data.getPath() + "---" + new String(data.getData()));
                            break;
                        case CHILD_UPDATED:
                            System.out.println("update ==== " + data.getPath() + "---" + new String(data.getData()));
                            break;
                        case CHILD_REMOVED:
                            System.out.println("delete ==== " + data.getPath() + "---" + new String(data.getData()));
                            break;
                            default:
                                System.out.println("default ==== " + data.getPath() + "---" + new String(data.getData()));
                                break;

                    }
                }
            }
        };
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        client.setData().forPath("/test","ssss".getBytes());
        client.create().forPath("/test/t_5","t1".getBytes());
        Thread.sleep(10000);
    }

    /**
     * node cache:监听节点本身的变化
     * @throws Exception
     */
    @Test
    public void bestListenterNodeCache() throws Exception {
        final NodeCache nodeCache = new NodeCache(client, "/test", false);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("path:" + nodeCache.getCurrentData().getPath() + "---data:" + new String(nodeCache.getCurrentData().getData()));
            }
        });

        nodeCache.start();
        client.setData().forPath("/test","ddddd".getBytes());
        client.create().forPath("/test/t_8","t1".getBytes());
        Thread.sleep(10000);
    }

        /**
         * tree cache :path cache和node cache的结合
         * @throws Exception
         */
    @Test
    public void bestListenterTreeCache() throws Exception {
        TreeCache treeCache = new TreeCache(client,"/test");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                  ChildData data = event.getData();
                if(data!=null){
                    switch(event.getType()){
                        case NODE_ADDED:
                            System.out.println("create ==== "+data.getPath()+"---"+new String(data.getData()));
                            break;
                        case NODE_UPDATED:
                            System.out.println("update ==== "+data.getPath()+"---"+new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("delete ==== "+data.getPath()+"---"+new String(data.getData()));
                            break;
                    }
                }
            }
        });

        treeCache.start();
        client.delete().forPath("/test/tes_1");
        Thread.sleep(1000);
        client.delete().forPath("/test/test_2");
        Thread.sleep(1000);
        client.setData().forPath("/test","feerf".getBytes());
        client.create().forPath("/test/test_0","Ee".getBytes());
        Thread.sleep(10000);
    }


    @Test
    public  void testTransaction() throws Exception {
        CuratorOp curatorOp1 = client.transactionOp().create().forPath("/test/tes_1","22".getBytes());
        CuratorOp curatorOp2 = client.transactionOp().create().forPath("/test/test_2", "223".getBytes());
        List<CuratorTransactionResult> results = client.transaction().forOperations(curatorOp1, curatorOp2);
        for (CuratorTransactionResult c:results) {
            System.out.println("结果是："+c.getForPath()+"---"+c.getType());

        }
    }

}
