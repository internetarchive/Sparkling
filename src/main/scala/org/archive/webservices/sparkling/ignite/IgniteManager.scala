package org.archive.webservices.sparkling.ignite

import java.io.File

import org.apache.ignite.configuration.{CacheConfiguration, DataStorageConfiguration, IgniteConfiguration}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.archive.webservices.sparkling.ignite.IgniteManager.nextName

trait IgniteManager {
  def ignite: Ignite

  def getWithConfig[K, V](name: String, config: CacheConfiguration[K, V] => Unit): IgniteCache[K, V] = {
    val cacheConfig = new CacheConfiguration[K, V](name)
    config(cacheConfig)
    ignite.getOrCreateCache(cacheConfig)
  }

  def getWithConfig[K, V](config: CacheConfiguration[K, V] => Unit): IgniteCache[K, V] = getWithConfig[K, V](nextName, config)

  def get[K, V](name: String): IgniteCache[K, V] = getWithConfig[K, V](name, (_: CacheConfiguration[K, V]) => {})

  def get[K, V]: IgniteCache[K, V] = get[K, V](nextName)
}

object IgniteManager extends IgniteManager {
  val DefaultSwapPath: String = "ignite_swap"

  private var nextCache: Int = 0

  private def nextName: String = {
    val name = nextCache.toString
    nextCache += 1
    name
  }

  var isolateNodes: Boolean = false

  lazy val storage: DataStorageConfiguration = {
    val config = new DataStorageConfiguration
    val region = config.getDefaultDataRegionConfiguration
    val swapPath = new File(DefaultSwapPath)
    swapPath.mkdirs()
    region.setSwapPath(swapPath.getCanonicalPath)
    region.setPersistenceEnabled(false)
    config
  }

  lazy val config: IgniteConfiguration = {
    val config = new IgniteConfiguration()
    config.setDataStorageConfiguration(storage)
    if (isolateNodes) {
      val spi = new TcpDiscoverySpi()
      spi.setIpFinder(new TcpDiscoveryVmIpFinder(true))
      config.setDiscoverySpi(spi)
    }
    config
  }

  private var _initialized = false
  def initialized: Boolean = _initialized

  lazy val ignite: Ignite = {
    _initialized = true
    Ignition.start(config)
  }
}
