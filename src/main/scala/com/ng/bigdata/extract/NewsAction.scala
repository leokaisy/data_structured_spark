package com.ng.bigdata.extract

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory


/**
  * @constructor 解析原始行为日志中的数据
  * @author QF
  * @date 2020/6/10 10:49 AM
  * @version V1.0
  */

class NewsAction extends java.io.Serializable{
  private val log = LoggerFactory.getLogger("NewsAction")

  def unionMetaAndBody(base64Line: String): String = {
    try {
      val textArray = base64Line.split("[-]")
      if (textArray.length != 2) {
        return null
      }
      // 拆分出meta数据
      val metaBytes = Base64.decodeBase64(textArray(0))
      val meta = new String(metaBytes)

      // 拆分出body数据
      val bodyBytes = Base64.decodeBase64(textArray(1))
      val body = new String(bodyBytes)

      // meta string to json object
      // 日志样例如下：
      // {"project":"news","ip":"39.106.208.130","ctime":1591843331592}
      val jsonMeta = JSON.parseObject(meta)

      // body string to json object
      // 样例如下
      // {"content":{"distinct_id":"51817820","event":"AppClick","properties":{"model":"iPhone6s plus","network_type":"4G","is_charging":"","app_version":"2.1","element_name":"分享","element_page":"首页","carrier":"中国电信","os":"Android","imei":"145007137881","battery_level":"36","screen_width":"640","screen_height":"320","device_id":"145007137881","client_time":"2020-06-11 10:42:11","ip":"171.15.249.136","manufacturer":"Apple"}}}
      val jsonBody = JSON.parseObject(body)

      // 从body中取出content
      val contentJson = jsonBody.getJSONObject("content")
      // 从content中取出properties
      val propJson = contentJson.getJSONObject("properties")

      // 定义一个空的Json对象，用来合并json meta和json body 中的字段，同时将body字段的多级结构展平为一级
      val jsonUnion = new JSONObject()
      // 取出所有properties中的字段
      jsonUnion.putAll(propJson.asInstanceOf[java.util.Map[String, _]])
      // 从body中删除properties，保留body中剩余的字段放置到新json对象中
      contentJson.remove("properties")
      // 将json body中删除properties后剩余的key放置到放置到新json对象中
      jsonUnion.putAll(contentJson.asInstanceOf[java.util.Map[String, _]])
      // 将meta json 中对象放置到新的json对象中
      jsonUnion.putAll(jsonMeta.asInstanceOf[java.util.Map[String, _]])
      val serverTime = jsonMeta.getBigInteger("ctime")

      // 将日志中服务器时间格式化
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val logday = sdf.format(serverTime)
      // 将logday字段添加到json对象中
      jsonUnion.put("logday", logday)
      // 最终的结果是，我们合并了meta信息和body信息，同时将meta和body的所有多级json结构，变为了一级。 即新的json中没有嵌套结构
      // 样例结果如下：
      // {"element_page":"新闻列表页","screen_width":"640","app_version":"2.1","os":"GNU/Linux","battery_level":"14","device_id":"463462211582","client_time":"2020-06-11 10:53:32","ip":"39.106.208.130","project":"news","is_charging":"1","manufacturer":"Apple","carrier":"中国电信","screen_height":"320","distinct_id":"51815228","imei":"463462211582","ctime":1591844012592,"model":"iPhone6s plus","network_type":"WIFI","event":"AppClick","element_name":"tag"}

      // 思考一下： 为什么要将body json的嵌套结构展平，当然因为我们的meta json本身就不是嵌套结构，所以无需展平，如果meta是嵌套结构这里也需要展平？
      jsonUnion.toJSONString

    } catch {
      case e: Exception =>
        log.error("data:"+base64Line+";"+e.getMessage,e)
        null
    }

  }
}

object NewsAction {

  def apply: NewsAction = new NewsAction()

}
