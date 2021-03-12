package org.bonitasoft.migrate.repair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class PlaySql {

  public String[] listDataSourcesBonitaEngine = new String[] { "java:/comp/env/bonitaSequenceManagerDS", // tomcat
      "java:jboss/datasources/bonitaSequenceManagerDS" }; // jboss

  // two way to use this clas : by a String connection of by a list of Datasource connection
  public enum TypeConnection {
    DIRECT, DATASOURCE
  };

  private TypeConnection typeConnection;
  private String urlConnection = null;
  private String[] dataSourcesConnection = null;

  private Connection currentConnection = null;
  static Logger logger = Logger.getLogger(PlaySql.class.getName());

  /*
   * H2:
   * jdbc:h2:file:D:/bonita/BPM-SP-7.7.3/workspace/Procergs-V5/h2_database/bonita_journal.db;MVCC=
   * TRUE;DB_CLOSE_ON_EXIT=FALSE;IGNORECASE=TRUE;AUTO_SERVER=TRUE;
   * Postgres jdbc:postgresql://localhost/test?user=fred&password=secret";
   */
  PlaySql(String urlConnection) {
    this.urlConnection = urlConnection;
    typeConnection = TypeConnection.DIRECT;
  }

  PlaySql(String[] dataSourcesConnection) {
    this.dataSourcesConnection = dataSourcesConnection;
    typeConnection = TypeConnection.DATASOURCE;
  }

  // then please call this at the begining (first time

  public void openPlaySql() {
    if (this.urlConnection == null)
      return;

    if (typeConnection == TypeConnection.DIRECT) {
      try {
        Class.forName("org.h2.Driver");

        currentConnection = DriverManager.getConnection(urlConnection);
      } catch (Exception e) {
        logger.severe("Open Connection [" + urlConnection + "] : " + e.getMessage());
      }
    }
  }

  public void closePlaySql() {
    if (typeConnection == TypeConnection.DIRECT) {
      try {
        currentConnection.close();
      } catch (Exception e) {
      }
    }
  }

  public void updateSql(String sqlRequest, List<Object> listSqlParameters) throws Exception {
    if (currentConnection == null)
      return;
    Connection con = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try {
      con = getConnection();
      pstmt = con.prepareStatement(sqlRequest);
      for (int i = 0; i < listSqlParameters.size(); i++) {
        pstmt.setObject(i + 1, listSqlParameters.get(i));
      }

      rs = pstmt.executeQuery();
      while (rs.next()) {

      }
    } catch (final Exception e) {
      final StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      final String exceptionDetails = sw.toString();
      logger.severe(".updateSql Error during sql [" + sqlRequest + "] : " + e.toString()
          + " : " + exceptionDetails);

    } finally {
      if (rs != null) {
        try {
          rs.close();
          rs = null;
        } catch (final SQLException localSQLException) {
        }
      }
      if (pstmt != null) {
        try {
          pstmt.close();
          pstmt = null;
        } catch (final SQLException localSQLException) {
        }
      }
      releaseDataSourceConnection(con);

    }
  }

  private void releaseDataSourceConnection(Connection con) throws SQLException {

    // datasource: set the connection to null to release it in the pool
    if (typeConnection == TypeConnection.DATASOURCE) {
      con.close();
      con = null;
    }

  }

  private Connection getConnection() throws SQLException {
    if (typeConnection == TypeConnection.DIRECT)
      return currentConnection;
    else {
      final DataSource dataSource = getDataSourceConnection();
      currentConnection = dataSource.getConnection();
      return currentConnection;
    }
  }

  private DataSource getDataSourceConnection() {
    // logger.info(loggerLabel+".getDataSourceConnection() start");
    String msg = "";

    List<String> listDatasourceToCheck = new ArrayList<String>();
    for (String dataSourceString : dataSourcesConnection)
      listDatasourceToCheck.add(dataSourceString);

    for (String dataSourceString : listDatasourceToCheck) {
      // logger.info(loggerLabel+".getDataSourceConnection() check["+dataSourceString+"]");
      try {
        final Context ctx = new InitialContext();
        final DataSource dataSource = (DataSource) ctx.lookup(dataSourceString);

        // logger.info(loggerLabel+".getDataSourceConnection() ["+dataSourceString+"] isOk");
        return dataSource;
      } catch (NamingException e) {
        // logger.info(loggerLabel+".getDataSourceConnection() error["+dataSourceString+"] : "+e.toString());
        msg += "DataSource[" + dataSourceString + "] : error " + e.toString() + ";";
      }
    }
    logger.severe("getDataSourceConnection: Can't found a datasource : " + msg);

    return null;
  }
}
