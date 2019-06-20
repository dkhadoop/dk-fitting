package com.dksou.fitting.ml.utils
import org.slf4j.LoggerFactory

import sys.process._

object RuntimeSUtils {
  private val logger = LoggerFactory.getLogger(RuntimeSUtils.getClass)
  def executeShell(cmd:String,logPath:String) = {
    logger.warn(logPath + " Task start  running ......")
    val status2 = cmd ! ProcessLogger(logger.warn _, logger.warn _)
    logger.warn(logPath + " Task execution completed !!! " + " Exit Code : " +  status2+" ")
    if(status2 != 0){
      throw new RuntimeException(logPath + " Execution Failed !!! Error Code : "  + status2)
    }

  }

}
