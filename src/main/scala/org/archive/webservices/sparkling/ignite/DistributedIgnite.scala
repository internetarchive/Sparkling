package org.archive.webservices.sparkling.ignite

import org.apache.ignite.Ignite

class DistributedIgnite private (init: => Unit) extends IgniteManager with Serializable {
  override def ignite: Ignite = {
    if (!IgniteManager.initialized) { IgniteManager.synchronized { if (!IgniteManager.initialized) init } }
    IgniteManager.ignite
  }
}

object DistributedIgnite {
  def apply(init: => Unit): DistributedIgnite = new DistributedIgnite(init)
}
