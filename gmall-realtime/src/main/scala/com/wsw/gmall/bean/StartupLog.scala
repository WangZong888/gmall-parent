package com.wsw.gmall.bean

case class Startuplog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      logType: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long
                     )
