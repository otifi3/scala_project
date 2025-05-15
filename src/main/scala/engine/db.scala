package engine

import java.sql.{Connection, DriverManager}

object db {
  private val url = "jdbc:postgresql://localhost:5432/scala?currentSchema=public"
  private val user = "otifi"
  private val password = "123"

  lazy val connection: Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(url, user, password)
  }
}
