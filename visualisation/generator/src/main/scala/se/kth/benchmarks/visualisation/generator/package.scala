package se.kth.benchmarks.visualisation

package object generator {
  implicit class JsCollectionOps(c: TraversableOnce[String]) {
    def mkJsString: String = c.map(v => s"'${v}'").mkString("[", ",", "]");
    def mkJsRawString: String = c.mkString("[", ",", "]");
  }
  implicit class JsArrayOps(c: Array[String]) {
    def mkJsString: String = c.map(v => s"'${v}'").mkString("[", ",", "]");
    def mkJsRawString: String = c.mkString("[", ",", "]");
  }
}
