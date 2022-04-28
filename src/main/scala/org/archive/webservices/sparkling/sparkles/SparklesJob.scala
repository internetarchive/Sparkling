package org.archive.webservices.sparkling.sparkles

import scala.reflect.ClassTag

abstract class SparklesJob[I, O: ClassTag] extends Serializable {
  def printLabel: Option[String] = None
  def input: I
  def process(iter: Int, input: I, print: Boolean = true): O

  protected var lastOut: Option[O] = None
  def get: O = lastOut.get

  def run(iter: Int, input: I, print: Boolean = true): O = {
    val out = process(iter, input, print)
    if (print && printLabel.isDefined) println("[" + printLabel.get + "]: " + out.toString)
    lastOut = Some(out)
    out
  }

  def map[A: ClassTag](label: String)(action: O => A): SparklesJob[I, A] = mapIter(label)((o, _) => action(o))
  def mapIter[A: ClassTag](label: String)(action: (O, Int) => A): SparklesJob[I, A] = mapIter(action, Some(label))

  def map[A: ClassTag](action: O => A, label: Option[String] = None): SparklesJob[I, A] = mapIter((o, _) => action(o), label)
  def mapIter[A: ClassTag](action: (O, Int) => A, label: Option[String] = None): SparklesJob[I, A] = {
    val jobParent = this
    val jobAction = action
    new SparklesJob[I, A] {
      @transient
      private val parent: SparklesJob[I, O] = jobParent
      @transient
      private val action: (O, Int) => A = jobAction
      override def printLabel: Option[String] = label
      def input: I = parent.input
      def process(iter: Int, in: I, print: Boolean): A = action(parent.run(iter, in, print), iter)
    }
  }
}

object SparklesJob {
  def init[I: ClassTag](inputLabel: Option[String], in: => I): SparklesJob[I, I] = new SparklesJob[I, I] {
    override def printLabel: Option[String] = inputLabel
    lazy val input: I = in
    def process(iter: Int, input: I, print: Boolean): I = input
    override def get: I = lastOut.getOrElse(input)
  }

  def apply[I: ClassTag](inputLabel: String)(in: => I): SparklesJob[I, I] = init(Some(inputLabel), in)

  def apply[I: ClassTag](in: => I): SparklesJob[I, I] = init(None, in)
}
