package dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnector {

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    PreparedStatement ps = null;
    PreparedStatement psUpdate = null;
    PreparedStatement myStmt = null;
    PreparedStatement dropPs = null;
    ResultSet myRs = null;

    Connection con =
        DriverManager.getConnection("jdbc:hsqldb:hsql://127.0.0.1:9001/test-db", "SA", "");


    if (con != null)
      con.close();
  }

}
