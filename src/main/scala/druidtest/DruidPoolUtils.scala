package druidtest

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.log4j.Logger

/**
  * Created by PerkinsZhu on 2019/2/20 17:23
  **/
object DruidPoolUtils {


  private val LOG = Logger.getLogger(DruidPoolUtils.getClass.getName)

  val dataSource: Option[DataSource] = {
    try {
      val druidProps = new Properties()
      // 获取Druid连接池的配置文件
      val druidConfig = getClass.getResourceAsStream("/druid.properties")
      // 倒入配置文件
      druidProps.load(druidConfig)
      Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case error: Exception =>
        LOG.error("Error Create Mysql Connection", error)
        None
    }
  }

  // 连接方式
  def getConnection: Option[Connection] = {
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None
    }
  }
}
