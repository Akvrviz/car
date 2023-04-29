package com.zsj

import java.text.SimpleDateFormat
import java.util.Date

object DateFormatTest {
  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date1 = new Date(1614716814000L)
    val date: String = dateFormat.format(date1)
    print(date)
  }
}
