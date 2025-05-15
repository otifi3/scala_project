//package engine
//
//import better.files._
//import java.util.Properties
//
//object Env {
//  private val envFile = File(".env")
//  private val props = new Properties()
//
//  if (envFile.exists) {
//    val reader = envFile.newBufferedReader
//    props.load(reader)
//    reader.close()
//  }
//
//  def get(key: String): String = props.getProperty(key)
//}
