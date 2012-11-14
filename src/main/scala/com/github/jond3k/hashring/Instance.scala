package com.github.jond3k.hashring

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class Instance(val id: String) {

  def this(host: String, port: Int) {
    this("%s_%s" format (host, port))
  }

  def this(host: String, port: Int, salt: String) {
    this("%s_%s_%s" format (host, port, salt))
  }

  override def hashCode = id.hashCode
  override def equals(o: Any) = o match {
    case t: Instance => t.id == id
    case _ => false
  }
  override def toString = "Instance(%s)" format id
}
