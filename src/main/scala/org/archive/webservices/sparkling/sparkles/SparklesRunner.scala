package org.archive.webservices.sparkling.sparkles

class SparklesRunner[I](from: => Option[(Int, I)]) {
  def loop[O](job: SparklesJob[I, O], print: Boolean = true, n: Int = -1)(loop: (Int, O) => Option[I]): Int = {
    val (fromIter, fromIn) = from.getOrElse { (0, job.input) }
    var iter = fromIter
    if (print) println("... running iteration " + iter + " ...")
    val out = job.run(iter, fromIn, print)
    var next = true
    while (next && (iter < n || n < 0)) {
      loop(iter, out) match {
        case Some(nextIn) =>
          iter += 1
          if (print) println("... running iteration " + iter + " ...")
          job.run(iter, nextIn, print)
        case None => next = false
      }
    }
    iter
  }

  def getLoop[O](job: SparklesJob[I, O], n: Int, print: Boolean = true)(loop: (Int, O) => Option[I]): O = {
    this.loop(job, print, n)(loop)
    job.get
  }
}

object SparklesRunner {
  def from[I, O](from: => Option[(Int, I)]): SparklesRunner[I] = { new SparklesRunner[I](from) }

  def loop[I, O](job: SparklesJob[I, O], print: Boolean = true, n: Int = -1)(loop: (Int, O) => Option[I]): Int = { new SparklesRunner[I](None).loop[O](job, print, n)(loop) }

  def getLoop[I, O](job: SparklesJob[I, O], n: Int, print: Boolean = true)(loop: (Int, O) => Option[I]): O = { new SparklesRunner[I](None).getLoop[O](job, n, print)(loop) }

  def get[I, O](job: SparklesJob[I, O]): O = job.run(0, job.input)
}
