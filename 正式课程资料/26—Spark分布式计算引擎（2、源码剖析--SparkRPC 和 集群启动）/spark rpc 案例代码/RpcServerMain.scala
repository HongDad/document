package org.apache.spark

import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.sql.SparkSession

/**
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Spark RPC 服务端
 */
object RpcServerMain {
    
    def main(args: Array[String]): Unit = {
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 SparkEnv
         */
        val conf: SparkConf = new SparkConf()
        val sparkSession = SparkSession.builder().config(conf).master("local[*]").appName("NX RPC").getOrCreate()
        val sparkContext: SparkContext = sparkSession.sparkContext
        val sparkEnv: SparkEnv = sparkContext.env
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 构建 RpcEnv
         */
        val rpcEnv = RpcEnv
            .create(HelloRpcSettings.getName(), HelloRpcSettings.getHostname(), HelloRpcSettings.getHostname(), HelloRpcSettings.getPort(), conf,
                sparkEnv.securityManager, 1, false)
        
        // TODO_MA 注释：创建 和启动 endpoint
        val helloEndpoint: RpcEndpoint = new HelloEndPoint(rpcEnv)
        // TODO_MA 注释： 通过 rpcEnv 的 setupEndpoint 方法来启动 RpcEndpoint
        // TODO_MA 注释： rpcEnv = actorySystem,  setupEndpoint = actorOf,  helloEndpoint = actor
        rpcEnv.setupEndpoint(HelloRpcSettings.getName(), helloEndpoint)
        
        rpcEnv.awaitTermination()
    }
}
