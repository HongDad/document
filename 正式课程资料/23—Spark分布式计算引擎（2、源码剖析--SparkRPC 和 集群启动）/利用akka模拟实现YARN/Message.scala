package com.mazh.scala.core.day4.rpc.yarn01

// TODO_MA 注释： 注册消息   nodemanager  -> resourcemanager
case class RegisterNodeManager(val nodemanagerid: String, val memory: Int, val cpu: Int)

// TODO_MA 注释： 注册完成消息 resourcemanager -> nodemanager
case class RegisteredNodeManager(val resourcemanagerhostname: String)

// TODO_MA 注释： 心跳消息  nodemanager -> resourcemanager
case class Heartbeat(val nodemanagerid: String)

// TODO_MA 注释： NodeManager 信息类
class NodeManagerInfo(val nodemanagerid: String, val memory: Int, val cpu: Int) {
    
    // TODO_MA 注释： 上一次心跳时间
    var lastHeartBeatTime: Long = _
    
    override def toString: String = {
        nodemanagerid + "," + memory + "," + cpu
    }
}

// TODO_MA 注释： 一个发送心跳的信号
case object SendMessage

// TODO_MA 注释： 一个检查信号
case object CheckTimeOut