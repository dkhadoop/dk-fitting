package com.dksou.fitting.graphx.utils

import com.google.common.hash.Hashing

/**
 * Created by Administrator on 2016/8/1 0001.
 */
object GuavaUtil {
  //将字符串数值化
  def hashId(str:String)={
    Hashing.md5().hashBytes(str.getBytes()).asLong()
  }
}
