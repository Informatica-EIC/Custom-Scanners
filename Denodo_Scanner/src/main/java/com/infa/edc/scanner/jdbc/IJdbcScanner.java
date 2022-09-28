/**
 * common interface for all JDBC based custom scanners
 * make extensions for specfic dbms types like athena, denodo & others
 */
package com.infa.edc.scanner.jdbc;

import java.sql.Connection;

/**
 * @author dwrigley
 *
 */
public interface IJdbcScanner {
	
	/**
	 * create a connection to the JDBC based database
	 * @param classType  full class of the JDBC driver - e.g. com.denodo.vdp.jdbc.Driver
	 * @param url connect string for the datbase
	 * @param user id used to connect to the database
	 * @param pwd password corresponding to the db id
	 * @return
	 */
	public Connection getConnection(String classType, String url, String user, String pwd);

}
