package javaapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * zk连接和权限
 */
public class TestCreateSession {
    private static final String SERVER = "192.168.128.149:2181";
    private static final int SESSION_TIMEOUT = 30000;
    private static Stat stat;

    @Test
    public void testSession1(){
        try {
            ZooKeeper zooKeeper = new ZooKeeper(SERVER,SESSION_TIMEOUT,null);
            System.out.println(zooKeeper.getState());
                        zooKeeper.create("/javaB","9:37".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            zooKeeper.delete("/javaB",1);
            // 自定义用户认证访问
            List<ACL> acls = new ArrayList<ACL>();  // 权限列表
            // 第一个参数是权限scheme，第二个参数是加密后的用户名和密码
            Id user1 = new Id("digest",makePwd("user1:123456"));
            acls.add(new ACL(ZooDefs.Perms.ALL, user1));  // 给予所有权限
//            acls.add(new ACL(ZooDefs.Perms.READ, user2));  // 只给予读权限
//            acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, user2));  // 多个权限的给予方式，使用 | 位运算符

            zooKeeper.setACL("/javaB",acls,-1);


            final List<ACL> aclList = zooKeeper.getACL("/javaB", stat);
            for (ACL acl:aclList) {
                System.out.println("权限scheme id：" + acl.getId());
                // 获取的是十进制的int型数字
                System.out.println("权限位：" + acl.getPerms());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取需要权限的内容
     */
    @Test
    public void testSessionAcl(){
        try {
            ZooKeeper zooKeeper = new ZooKeeper(SERVER,SESSION_TIMEOUT,null);
            System.out.println(zooKeeper.getState());
            //给当前会话添加一个用户
            zooKeeper.addAuthInfo("digest","super:admin".getBytes());
            System.out.println(new String(zooKeeper.getData("/javaB",false,null)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String makePwd(String up){
        try {
            return DigestAuthenticationProvider.generateDigest(up);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Test
    public void testSession2(){
        try {
            ZooKeeper zooKeeper = new ZooKeeper(SERVER, SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent event) {
                    countDownLatch.countDown();
                    System.out.println("got connection");
                }
            });
            countDownLatch.await();
            System.out.println(zooKeeper.getState());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
