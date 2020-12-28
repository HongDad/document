package com.mazh.scala.core.day4.rpc.yarn01

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 集群主节点抽象
 *  1、receive 方法        接收其他 actor 发送过来的消息，然后进行模式匹配，进行消息处理，有可能返回消息
 *  2、preStart() 方法     对象在构建成功之后，就会触发执行 preStart
 *  3、postStop 方法       在对象销毁之前，会执行一次
 *  -
 *  必须了解的知识：
 *  1、伴生类 class A 和 伴生对象 object A（定义的方法，都是静态方法）
 *  2、关于 scala 中定义的一个类的构造方法：
 *      构造器： 类名后面的括号
 *      代码实现： {} 中的一切能执行的代码
 *          变量的初始化
 *          代码块
 *          静态代码块
 *          不能执行的代码： 定义的方法（未调用， 内部类）
 */
class MyResourceManager(var hostname: String, var port: Int) extends Actor {
    
    // TODO_MA 注释： 用来存储每个注册的NodeManager节点的信息
    private var id2nodemanagerinfo = new mutable.HashMap[String, NodeManagerInfo]()
    
    // TODO_MA 注释： 对所有注册的NodeManager进行去重，其实就是一个HashSet
    private var nodemanagerInfoes = new mutable.HashSet[NodeManagerInfo]()
    
    // TODO_MA 注释： actor在最开始的时候，会执行一次
    override def preStart(): Unit = {
        import scala.concurrent.duration._
        import context.dispatcher
        
        // TODO_MA 注释： 调度一个任务， 每隔五秒钟执行一次
        context.system.scheduler.schedule(0 millis, 5000 millis, self, CheckTimeOut)
    }
    
    // TODO_MA 注释： 正经服务方法
    override def receive: Receive = {
        
        // TODO_MA 注释： 接收 注册消息
        case RegisterNodeManager(nodemanagerid, memory, cpu) => {
            val nodeManagerInfo = new NodeManagerInfo(nodemanagerid, memory, cpu)
            println(s"节点 ${nodemanagerid} 上线")
            
            // TODO_MA 注释： 对注册的NodeManager节点进行存储管理
            id2nodemanagerinfo.put(nodemanagerid, nodeManagerInfo)
            nodemanagerInfoes += nodeManagerInfo
            
            // TODO_MA 注释： 把信息存到zookeeper
            // TODO_MA 注释： sender() 谁给我发消息，sender方法返回的就是谁
            sender() ! RegisteredNodeManager(hostname + ":" + port)
        }
        
            // TODO_MA 注释： 接收心跳消息
        case Heartbeat(nodemanagerid) => {
            val currentTime = System.currentTimeMillis()
            val nodeManagerInfo = id2nodemanagerinfo(nodemanagerid)
            nodeManagerInfo.lastHeartBeatTime = currentTime
            
            id2nodemanagerinfo(nodemanagerid) = nodeManagerInfo
            nodemanagerInfoes += nodeManagerInfo
        }
        
        // TODO_MA 注释： 检查过期失效的 NodeManager
        case CheckTimeOut => {
            val currentTime = System.currentTimeMillis()
            
            // TODO_MA 注释： 15 秒钟失效
            nodemanagerInfoes.filter(nm => {
                val heartbeatTimeout = 15000
                val bool = currentTime - nm.lastHeartBeatTime > heartbeatTimeout
                if (bool) {
                    println(s"节点 ${nm.nodemanagerid} 下线")
                }
                bool
            }).foreach(deadnm => {
                nodemanagerInfoes -= deadnm
                id2nodemanagerinfo.remove(deadnm.nodemanagerid)
            })
            println("当前注册成功的节点数" + nodemanagerInfoes.size + "\t分别是：" + nodemanagerInfoes.map(x => x.toString)
              .mkString(","));
        }
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 启动入口， 伴生对象！
 */
object MyResourceManager {
    def main(args: Array[String]): Unit = {
        
        // TODO_MA 注释： 地址参数
        val str =
        s"""
           |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
           |akka.remote.netty.tcp.hostname = localhost
           |akka.remote.netty.tcp.port = 6789
      """.stripMargin
        val conf = ConfigFactory.parseString(str)
        
        // TODO_MA 注释：ActorSystem
        val actorSystem = ActorSystem(Constant.RMAS, conf)
        
        // TODO_MA 注释：启动了一个actor ： MyResourceManager
        actorSystem.actorOf(Props(new MyResourceManager("localhost", 6789)), Constant.RMA)
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： actor 的生命周期
         *  1、MyResourceManager actor 的构造方法
         *  2、preStart()  当 actor 实例创建成功的时候，就会马上调用这个 actor 的 preStart() 来执行
         */
    }
}
