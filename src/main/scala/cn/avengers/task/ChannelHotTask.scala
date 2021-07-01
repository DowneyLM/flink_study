package cn.avengers.task

import cn.avengers.bean.ClickLogWide
import cn.avengers.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Author ZengZihang
 * Desc 实时频道热点统计分析
 */
object ChannelHotTask {

  //定义一个样例类，用来封装频道id和访问次数
  case class ChannelRealHot(channelId:String, visited:Long)

  def process(clinkLogWideDS : DataStream[ClickLogWide]) = {

    //每隔10s统计一次各个频道对应的访问量，并将结果和历史数据合并，存入HBase
    //也就是说使用Hbase存放各个频道的实时访问量，每隔10s更新一次
    import org.apache.flink.streaming.api.scala._
    val currentResult: DataStream[ChannelRealHot] = clinkLogWideDS.map(log => {
      ChannelRealHot(log.channelID, log.count)
    }).keyBy(_.channelId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce((a,b) => {
        ChannelRealHot(a.channelId,a.visited + b.visited)
      })

    currentResult.addSink(new SinkFunction[ChannelRealHot] {
      override def invoke(value: ChannelRealHot, context: SinkFunction.Context) = {

        //1.先查HBase该频道的上次的访问次数
        val tableName = "channel_realhot"
        val rowkey = value.channelId
        val columnFamily = "info"
        val queryColumn = "visited"

        //查出历史值（指定频道的访问次数历史值）
        //去HBase的channel_realHot表的info列族中根据ChannelId查询的列visited
        val historyVisited: String = HBaseUtil.getData(tableName, rowkey, columnFamily, queryColumn)

        var resultVisited = 0L

        if(StringUtils.isBlank(historyVisited)){
          resultVisited = value.visited
        }else{
          resultVisited = value.visited + historyVisited.toLong
        }

        //存入Hbase
        HBaseUtil.putData(tableName,rowkey,columnFamily,queryColumn,resultVisited.toString)
      }

    })


  }

}
