package engine

import java.sql.{Connection, DriverManager}

/**
 * Object `db` provides a singleton PostgreSQL database connection.
 *
 * It initializes a JDBC connection to a local PostgreSQL database with:
 * - URL: jdbc:postgresql://localhost:5432/scala?currentSchema=public
 * - User: otifi
 * - Password: 123
 *
 * The connection is lazy-initialized on first access and reused thereafter.
 * The PostgreSQL JDBC driver class is loaded explicitly.
 *
 * Usage:
 *   Access `db.connection` to obtain a live JDBC connection.
 *
 * Note:
 * - Ensure PostgreSQL server is running and accessible at the specified URL.
 * - Credentials must be valid for successful connection.
 */
object db {
  private val url = "jdbc:postgresql://localhost:5432/scala?currentSchema=public"
  private val user = "otifi"
  private val password = "123"

  lazy val connection: Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(url, user, password)
  }
}
