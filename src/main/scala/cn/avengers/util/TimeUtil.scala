package cn.avengers.util

import org.apache.commons.lang3.time.FastDateFormat

/**
 * Author ZengZihang
 * Desc
 */
object TimeUtil {

  def parseTime(timestamp: Long, pattern: String) : String = {
    FastDateFormat.getInstance(pattern).format(timestamp)
  }

  /**
   * 比对时间
   * @param currentTime 当前时间
   * @param historyTime 历史时间
   * @param format      时间格式 yyyyMM yyyyMMdd
   * @return            1或者0
   */
  def compareDate(currentTime : Long, historyTime : Long, format : String) : Int = {

    val currentTimeStr = parseTime (currentTime, format)
    val histroyTimeStr = parseTime(historyTime, format)

    //比对字符串大小，如果当前时间 > 历史时间 返回1
    var result = currentTimeStr.compareTo(histroyTimeStr)

    if(result > 0) {
      result = 1
    }else{
      result = 0
    }
    result
  }

}