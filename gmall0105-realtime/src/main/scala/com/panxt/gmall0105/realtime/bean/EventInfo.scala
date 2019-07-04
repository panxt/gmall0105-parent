package com.panxt.gmall0105.realtime.bean

/**
  * @author panxt
  */
case class EventInfo (mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      evid:String ,
                      pgid:String ,
                      npgid:String ,
                      itemid:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     ) {

}
