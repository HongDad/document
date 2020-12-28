package com.mazh.scala.core.day4.rpc.yarn01

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 *  1、spark rpc 生命周期方法： onStart receive onStop
 *  2、akka  rpc 生命周期方法： preStart receive postStop()
 */
class MyNodeManager(val nmhostname: String, val resourcemanagerhostname: String, val resourcemanagerport: Int, val memory: Int,
    val cpu: Int) extends Actor {
    
    var nodemanagerid: String = nmhostname
    var rmRef: ActorSelection = _
    
    // TODO_MA 注释： 会提前执行一次
    // TODO_MA 注释： 当前NM启动好了之后，就应该给 RM 发送一个注册消息
    // TODO_MA 注释： 发给谁，就需要获取这个谁的一个ref实例
    override def preStart(): Unit = {
        
        // TODO_MA 注释： 获取消息发送对象的一个ref实例
        // 远程path　　                  akka.tcp://（ActorSystem的名称）@（远程地址的IP）   ：         （远程地址的端口）/user/（Actor的名称）
        rmRef = context.actorSelection(s"akka.tcp://${
            Constant.RMAS
        }@${resourcemanagerhostname}:${resourcemanagerport}/user/${Constant.RMA}")
        
        // TODO_MA 注释： 发送消息
        println(nodemanagerid + " 正在注册")
        rmRef ! RegisterNodeManager(nodemanagerid, memory, cpu)
    }
    
    // TODO_MA 注释： 正常服务方法
    override def receive: Receive = {
        
        // TODO_MA 注释： 接收到注册成功的消息
        case RegisteredNodeManager(masterURL) => {
            println(masterURL);
            
            // TODO_MA 注释： initialDelay: FiniteDuration, 多久以后开始执行
            // TODO_MA 注释： interval:     FiniteDuration, 每隔多长时间执行一次
            // TODO_MA 注释： receiver:     ActorRef, 给谁发送这个消息
            // TODO_MA 注释： message:      Any  发送的消息是啥
            import scala.concurrent.duration._
            import context.dispatcher
            context.system.scheduler.schedule(0 millis, 4000 millis, self, SendMessage)
        }
        
        // TODO_MA 注释： 发送心跳
        case SendMessage => {
            // TODO_MA 注释： 向主节点发送心跳信息
            rmRef ! Heartbeat(nodemanagerid)
            
            println(Thread.currentThread().getId)
        }
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 启动类
 *  运行的时候，需要制定参数:
 *  localhost localhost 6789 64 32 9911 bigdata02
 */
object MyNodeManager {
    def main(args: Array[String]): Unit = {
        
        // TODO_MA 注释： 远程主机名称
        val HOSTNAME = args(0)
        
        // TODO_MA 注释：  RM 的 hostname 和 port
        val RM_HOSTNAME = args(1)
        val RM_PORT = args(2).toInt
        
        // TODO_MA 注释：  抽象的内存资源 和 CPU 个数
        val NODEMANAGER_MEMORY = args(3).toInt
        val NODEMANAGER_CORE = args(4).toInt
        
        // TODO_MA 注释：  当前 NM 的 hostname 和 port
        var NODEMANAGER_PORT = args(5).toInt
        var NMHOSTNAME = args(6)
        
        // TODO_MA 注释：  指定主机名称和端口号相关的配置
        val str =
            s"""
               |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
               |akka.remote.netty.tcp.hostname = ${HOSTNAME}
               |akka.remote.netty.tcp.port = ${NODEMANAGER_PORT}
            """.stripMargin
        val conf = ConfigFactory.parseString(str)
        
        // TODO_MA 注释： 启动一个 ActorSystem
        val actorSystem = ActorSystem(Constant.NMAS, conf)
        
        // TODO_MA 注释： 启动一个Actor
        actorSystem.actorOf(Props(new MyNodeManager(NMHOSTNAME, RM_HOSTNAME, RM_PORT, NODEMANAGER_MEMORY, NODEMANAGER_CORE)), Constant.NMA)
    }
}