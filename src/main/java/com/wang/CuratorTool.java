package com.wang;

import java.util.concurrent.CountDownLatch;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * 
 * zookeeper如何保证强一致性的：
 * 1.顺序一致性 	来自任意特定客户端的更新都会按其发送顺序被提交。也就是说，如果一个客户端将Znode z的值更新为a，在之后的操作中，
 * 				它又将z的值更新为b，则没有客户端能够在看到z的值是b之后再看到值a（如果没有其他对z的更新）
 * 
 * 2.原子性                每个更新要么成功，要么失败。这意味着如果一个更新失败，则不会有客户端会看到这个更新的结果。
 * 
 * 3.单一系统映像     一 个客户端无论连接到哪一台服务器，它看到的都是同样的系统视图。
 * 				这意味着，如果一个客户端在同一个会话中连接到一台新的服务器，它所看到的系统状态不会比 在之前服务器上所看到的更老。
 * 					当一台服务器出现故障，导致它的一个客户端需要尝试连接集合体中其他的服务器时，
 * 				所有滞后于故障服务器的服务器都不会接受该 连接请求，除非这些服务器赶上故障服务器。
 * 
 * 
 * ZAB协议  ：1.消息广播   （FIFO 事务请求有唯一ID，顺序执行；且采用二阶段提交（半数ok就提交））
 * 		   2.崩溃恢复 （重新选举leader）
 * 
 * 
 * 
 * 
 * 
 * @author wanghe
 *
 */
public abstract class CuratorTool {
	
	public static void main(String[] args)  {
        String a = "hello2";
        final  String b = "hello";
        String d = "hello";
        String c = b + 2;
        String e = d +2;
        System.out.println((a == c));
        System.out.println((a == e));
    }
	/**
	 * 同步操作zookeeper的节点
	 * @throws Exception
	 */
	public void fun1() throws Exception {
		RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework=CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 300, retryPolicy);
		curatorFramework.start();
		//创建一个节点
		curatorFramework.create().forPath("/wang","the world is so beautiful".getBytes());
		//创建一个临时节点
		curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath("/short", "a moment".getBytes());
		//创建顺序节点，返回完整名称（带有序号），利用这个特性，可以做全局id
		String data=curatorFramework.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/short", "a moment".getBytes());
		data.toString();
		//删除一个节点
		curatorFramework.delete().forPath("/wang");
		//递归删除所有子节点
		curatorFramework.delete().deletingChildrenIfNeeded().forPath("/parent");
		//获取节点的数据
		byte[]bytes= curatorFramework.getData().forPath("/wang");
		System.err.print(new String(bytes));
		//更新数据
		curatorFramework.setData().withVersion(-1).forPath("/short","just two minitue".getBytes());
	}

	/**
	 * 异步创建节点
	 * @throws Exception
	 */
	public void fun2() throws Exception {
		RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework=CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 300, retryPolicy);
		curatorFramework.start();
		//异步创建节点
		curatorFramework.create().inBackground(new BackgroundCallback() {
			
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
				client.getData().forPath("/xx");
				if(CuratorEventType.CREATE.equals(event.getType()) ) {
					System.err.println("创建成功");
				}
			}
		}).forPath("/parent");
	}
	
	/**
	 * 节点的监听（nodecache）
	 * @throws Exception
	 */
	public void fun3() throws Exception {
		RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework=CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 300, retryPolicy);
		curatorFramework.start();
		//创建一个节点
		curatorFramework.create().forPath("/wang","the world is so beautiful".getBytes());
		
		final NodeCache nodeCache=new NodeCache(curatorFramework, "/wang", false);
		nodeCache.start(true);
		nodeCache.getListenable().addListener(new NodeCacheListener() {
			
			@Override
			public void nodeChanged() throws Exception {
				System.err.print("节点被修改了，新内容："+new String(nodeCache.getCurrentData().getData()));
			}
		});
		
		
		Thread.sleep(Integer.MAX_VALUE);
		nodeCache.close();
	}
	
	
	/**
	 * master选举 
	 * (如果多个业务服务器节点同时执行该方法,同时去一个根节点去创建一个名称一样的子节点，谁创建成功，谁就是master)
	 * @throws InterruptedException 
	 */
	public void fun4() throws InterruptedException {
		String rootPath="/select_root_path";
		RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework=CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 300, retryPolicy);
		curatorFramework.start();
		
		LeaderSelector leaderSelector=new LeaderSelector(curatorFramework, rootPath, new LeaderSelectorListenerAdapter() {
			
			@Override
			public void takeLeadership(CuratorFramework framework) throws Exception {
				System.err.print("成为了master角色");
			}
		});
		leaderSelector.autoRequeue();
		leaderSelector.start();
		Thread.sleep(Integer.MAX_VALUE);
		leaderSelector.close();
	}
	
	/**
	 * 分布式锁
	 */
	public void fun5() {
		String path="/distribute_lock_path";
		RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFramework=CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 300, retryPolicy);
		curatorFramework.start();
		InterProcessMutex lock=new InterProcessMutex(curatorFramework, path);
		//使用countDownlatch是想模拟让所有线程都启动后，再去抢锁
		CountDownLatch countDownLatch=new CountDownLatch(1);
		
		for(int i=0;i<30;i++) {
			new Thread(()-> {
				try {
					countDownLatch.await();
					lock.acquire();
				} catch (Exception e) { 
					e.printStackTrace();
				}
				System.err.println("=================执行业务代码1==================");
				System.err.println("=================执行业务代码2==================");
				System.err.println("=================执行业务代码3==================");
				try {
					lock.release();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();
		}
		countDownLatch.countDown();
	}
	/********************zookeeper的应用场景*******************************/
	
	/**
	 * 1.发布/订阅
	 * 
	 * 因为监听机制，如果多个客户端监听一个节点，若节点数据修改了，所有客户端都会触发监听事件， 
	 * 利用这一点可以做发布/订阅，（具体业务里还可以进行 做配置中心）
	 */
	
	/**
	 * 2.全局ID （通过创建顺序节点，返回值中会返回节点的完整名字，完整信息中就有序号）
	 */
	
	/**
	 * 3.分布式锁 (上面已经实现)
	 */
	/**
	 * 4.master选举 (上面已经实现)
	 * 
	 */
	
	/**
	 * 5.分布式系统的机器间通信
	 * (三点  ： 心跳检测  、工作进度汇报 、系统调度)
	 * 
	 * （1）心跳检测               不同机器在指定的节点下创建临时节点，然后不同的机器节点可以根据临时节点来判断对应的客户端机器是否存活
	 * （2）工作进度汇报       任务客户端都在指定节点下创建临时节点，并将进度写入这个临时节点
	 * （3）系统调度  	       分布式系统通常由一个控制台和若干客户端组成；控制台负责将一系列指令分发到客户端执行
	 * 				       实际上就是控制台修改zookeeper的某些节点数据，然后把这些数据变更以事件的形式通知给对应的订阅客户端
	 */
	/**
	 * 6.分布式对列  
	 * （1）FIFO  顺序临时节点
	 */
}
