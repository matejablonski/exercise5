package dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnector {

  public static void main(String[] args) throws SQLException {

    Connection con = null;

    try {
      con = getConnection();
      clearDatabase(con);
      createTableStudent(con);
      createTableFaculty(con);
      createTableClass(con);
      createTableEnrollment(con);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (con != null)
        con.close();
    }


  }

  private static Connection getConnection() throws SQLException {
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    Connection con =
        DriverManager.getConnection("jdbc:hsqldb:hsql://127.0.0.1:9001/test-db", "SA", "");
    return con;
  }

  private static void clearDatabase(Connection con) throws SQLException {
    PreparedStatement ps = null;
    try {
      ps = con.prepareStatement("DROP SCHEMA PUBLIC CASCADE");
      ps.execute();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (ps != null)
        ps.close();
    }

  }

  private static void createTableStudent(Connection con) throws SQLException {
    PreparedStatement ps = null;
    ResultSet myRs = null;

    String createTableSQL = "CREATE TABLE Student (" + "pkey INTEGER NOT NULL,"
        + "name VARCHAR(50) NOT NULL," + "sex VARCHAR(6) NOT NULL," + "age INTEGER NOT NULL,"
        + "level INTEGER NOT NULL, " + "PRIMARY KEY (pkey));";

    String query = "INSERT INTO Student" + "(pkey, name, sex, age, level) VALUES" + "(?,?,?,?,?)";


    ps = con.prepareStatement(createTableSQL);
    ps.execute();
    ps = con.prepareStatement(query);
    ps.setInt(1, 1);
    ps.setString(2, "John Smith");
    ps.setString(3, "male");
    ps.setInt(4, 23);
    ps.setInt(5, 2);
    ps.execute();

    ps.setInt(1, 2);
    ps.setString(2, "Rebecca Milson");
    ps.setString(3, "famale");
    ps.setInt(4, 27);
    ps.setInt(5, 1);
    ps.execute();

    ps.setInt(1, 3);
    ps.setString(2, "George Heartbreaker");
    ps.setString(3, "male");
    ps.setInt(4, 19);
    ps.setInt(5, 1);
    ps.execute();

    ps.setInt(1, 4);
    ps.setString(2, "Deepika Chopra");
    ps.setString(3, "famale");
    ps.setInt(4, 25);
    ps.setInt(5, 3);
    ps.execute();


    ps = con.prepareStatement("SELECT * FROM Student");
    myRs = ps.executeQuery();

    while (myRs.next()) {
      int pkey = myRs.getInt("pKey");
      String name = myRs.getString("name");
      String sex = myRs.getString("sex");
      int age = myRs.getInt("age");
      int level = myRs.getInt("level");

      System.out.printf("%d, %s, %s, %d, %d\n", pkey, name, sex, age, level);
    }

    if (ps != null)
      ps.close();
    if (myRs != null)
      myRs.close();
  }

  private static void createTableFaculty(Connection con) throws SQLException {
    PreparedStatement ps = null;
    ResultSet myRs = null;

    String createTableSQL = "CREATE TABLE Faculty (" + "pkey INTEGER NOT NULL,"
        + "name VARCHAR(50) NOT NULL," + "PRIMARY KEY (pkey));";
    String query = "INSERT INTO Faculty" + "(pkey, name) VALUES" + "(?,?)";

    ps = con.prepareStatement(createTableSQL);
    ps.execute();
    ps = con.prepareStatement(query);

    ps.setInt(1, 100);
    ps.setString(2, "Engineering");
    ps.execute();

    ps.setInt(1, 101);
    ps.setString(2, "Philosophy");
    ps.execute();

    ps.setInt(1, 102);
    ps.setString(2, "Law and administration");
    ps.execute();

    ps.setInt(1, 103);
    ps.setString(2, "Languages");
    ps.execute();

    ps = con.prepareStatement("SELECT * FROM Faculty");
    myRs = ps.executeQuery();

    System.out.println();
    while (myRs.next()) {
      int pkey = myRs.getInt("pKey");
      String name = myRs.getString("name");

      System.out.printf("%d, %s,\n", pkey, name);
    }

    if (ps != null)
      ps.close();
    if (myRs != null)
      myRs.close();
  }

  private static void createTableClass(Connection con) throws SQLException {
    PreparedStatement ps = null;
    ResultSet myRs = null;

    String createTableSQL = "CREATE TABLE Class (" + "pkey INTEGER NOT NULL,"
        + "name VARCHAR(50) NOT NULL," + "fkey_faculty INTEGER NOT NULL," + "PRIMARY KEY (pkey),"
        + "FOREIGN KEY (fkey_faculty) REFERENCES Faculty(pkey) );";
    String query = "INSERT INTO Class" + "(pkey, name, fkey_faculty) VALUES" + "(?,?,?)";

    ps = con.prepareStatement(createTableSQL);
    ps.execute();
    ps = con.prepareStatement(query);

    ps.setInt(1, 1000);
    ps.setString(2, "Introduction to labour law");
    ps.setInt(3, 102);
    ps.execute();

    ps.setInt(1, 1001);
    ps.setString(2, "Graph algorithms");
    ps.setInt(3, 100);
    ps.execute();

    ps.setInt(1, 1002);
    ps.setString(2, "Existentialism in 20th century");
    ps.setInt(3, 101);
    ps.execute();

    ps.setInt(1, 1003);
    ps.setString(2, "English grammar");
    ps.setInt(3, 103);
    ps.execute();

    ps.setInt(1, 1004);
    ps.setString(2, "From Plato to Kant");
    ps.setInt(3, 101);
    ps.execute();

    ps = con.prepareStatement("SELECT * FROM Class");
    myRs = ps.executeQuery();

    System.out.println();
    while (myRs.next()) {
      int pkey = myRs.getInt("pkey");
      String name = myRs.getString("name");
      int fkey_faculty = myRs.getInt("fkey_faculty");

      System.out.printf("%d, %s, %d\n", pkey, name, fkey_faculty);
    }

    if (ps != null)
      ps.close();
    if (myRs != null)
      myRs.close();
  }

  private static void createTableEnrollment(Connection con) throws SQLException {
    PreparedStatement ps = null;
    ResultSet myRs = null;

    String createTableSQL = "CREATE TABLE Enrollment (" + "pkey INTEGER NOT NULL,"
        + "fkey_student INTEGER NOT NULL," + "fkey_class INTEGER NOT NULL,"
        + "FOREIGN KEY (fkey_student) REFERENCES Student(pkey),"
        + "FOREIGN KEY (fkey_class) REFERENCES Class(pkey) );";
    String query = "INSERT INTO Enrollment" + "(pkey, fkey_student, fkey_class) VALUES" + "(?,?,?)";

    ps = con.prepareStatement(createTableSQL);
    ps.execute();
    ps = con.prepareStatement(query);

    ps.setInt(1, 1);
    ps.setInt(2, 1);
    ps.setInt(3, 1000);
    ps.execute();

    ps.setInt(1, 2);
    ps.setInt(2, 1);
    ps.setInt(3, 1002);
    ps.execute();

    ps.setInt(1, 3);
    ps.setInt(2, 1);
    ps.setInt(3, 1003);
    ps.execute();

    ps.setInt(1, 4);
    ps.setInt(2, 1);
    ps.setInt(3, 1004);
    ps.execute();

    ps.setInt(1, 5);
    ps.setInt(2, 2);
    ps.setInt(3, 1002);
    ps.execute();

    ps.setInt(1, 6);
    ps.setInt(2, 2);
    ps.setInt(3, 1003);
    ps.execute();

    ps.setInt(1, 7);
    ps.setInt(2, 4);
    ps.setInt(3, 1000);
    ps.execute();

    ps.setInt(1, 8);
    ps.setInt(2, 4);
    ps.setInt(3, 1002);
    ps.execute();

    ps.setInt(1, 9);
    ps.setInt(2, 4);
    ps.setInt(3, 1003);
    ps.execute();

    ps = con.prepareStatement("SELECT * FROM Enrollment");
    myRs = ps.executeQuery();

    System.out.println();
    while (myRs.next()) {
      int fkey_student = myRs.getInt("fkey_student");
      int fkey_class = myRs.getInt("fkey_class");

      System.out.printf("%d, %d\n", fkey_student, fkey_class);
    }

    if (ps != null)
      ps.close();
    if (myRs != null)
      myRs.close();
  }
}
