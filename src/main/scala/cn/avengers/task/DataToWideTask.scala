package cn.avengers.task

import cn.avengers.bean.{ClickLogWide, Message}
import cn.avengers.util.{HBaseUtil, TimeUtil}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.StringUtils._
import org.apache.flink.streaming.api.scala.DataStream


/**
 * Author ZengZihang
 * Desc flink-task 将message转为ClickLogWide
 */
object DataToWideTask {

  def process(messageDS : DataStream[Message]): DataStream[ClickLogWide] = {

    import org.apache.flink.streaming.api.scala._
    messageDS.map(
      msg => {
        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city
        val yearMonth = TimeUtil.parseTime(msg.timeStamp,"yyyyMM")
        val yearMonthDay = TimeUtil.parseTime(msg.timeStamp,"yyyyMMdd")
        val yearMonthDayHour = TimeUtil.parseTime(msg.timeStamp,"yyyyMMddHH")
        val (isNew, isHourNew, isDayNew, isMonthNew) = getIsNew(msg)

        ClickLogWide(
          msg.clickLog.channelID,
          msg.clickLog.categoryID,
          msg.clickLog.produceID,
          msg.clickLog.country,
          msg.clickLog.province,
          msg.clickLog.city,
          msg.clickLog.network,
          msg.clickLog.source,
          msg.clickLog.browserType,
          msg.clickLog.entryTime,
          msg.clickLog.leaveTime,
          msg.clickLog.userID,
          msg.count,
          msg.timeStamp,
          address,
          yearMonth,
          yearMonthDay,
          yearMonthDayHour,
          isNew,
          isHourNew,
          isDayNew,
          isMonthNew
        )
      }
    )

  }


  def getIsNew(msg:Message) = {

    //0表示不是新用户，是老用户
    //1表示是新用户
    var isNew = 0
    var isHournew = 0
    var isDayNew = 0
    var isMonthNew = 0

    //根据用户访问的频道id，用户id，时间戳来判断用户是否是该时间段的新用户


    //首先得去Hbase中查询该用户访问该频道的上一次访问时间
    //定义一些Hbase的常量，如表名，列族名，字段名
    val tableName = "user_history"
    val columnFamily = "info"
    val rowkey = msg.clickLog.userID + ":" + msg.clickLog.channelID
    val queryColumn = "lastVisitTime"

    //去Hbase的user_history表中的info列族根据rowkey(用户id+频道)查询lastVisitTime
    val lastVisitTime: String = HBaseUtil.getData(tableName, rowkey, columnFamily, queryColumn)

    if(isBlank(lastVisitTime)){
      isNew = 1
      isHournew = 1
      isDayNew = 1
      isMonthNew = 1
    }else{
      //说明有记录该用户访问该频道的上次访问时间,说明是老用户,但是不确定是否是某个时间段的老用户,需要判断时间
      //如该用户访问该频道的这次访问时间为 2021 01 01 12 ,上次访问时间为 2021 01 01 11 ,则是新用户
      //如该用户访问该频道的这次访问时间为 2021 01 02,上次访问时间为 2021 01 01  ,则是新用户
      //如该用户访问该频道的这次访问时间为 2021 02 ,上次访问时间为 2021 01   ,则是新用户
      isNew = 0
      isHournew = TimeUtil.compareDate(msg.timeStamp,lastVisitTime.toLong,"yyyyMMddHH")
      //当前时间比历史事件大，返回1，表示新用户
      isDayNew = TimeUtil.compareDate(msg.timeStamp, lastVisitTime.toLong, "yyyyMMdd")
      isMonthNew = TimeUtil.compareDate(msg.timeStamp, lastVisitTime.toLong, "yyyyMM")
    }

    //注意：把这一次访问的时间存储到Hbase中，作为该用户访问该频道的上一次访问时间
    HBaseUtil.putData(tableName,rowkey,columnFamily,queryColumn,msg.timeStamp.toString)

    (isNew,isHournew,isDayNew,isMonthNew)
  }

}
