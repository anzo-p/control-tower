package helpers

object Extensions {
  implicit class EnvOps(env: Map[String, String]) {

    def getOrThrow(key: String, errMsg: String): String = {
      env.getOrElse(key, throw new IllegalArgumentException(errMsg))
    }
  }
}
