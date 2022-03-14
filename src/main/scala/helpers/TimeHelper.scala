package helpers

/**
  * Created by Matthew on 12/03/2021.
  */
object TimeHelper {
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: "+(System.nanoTime-s)/1e6+"ms")
    ret
  }
}
