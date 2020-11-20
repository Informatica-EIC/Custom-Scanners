/**
 * 
 */
package com.infa.edc.scanner.denodo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author dwrigley
 * 
 * this class is required to keep track of all of the datasource definitions
 * especially when generating links to non jdbc sources like DS, WS, XML, JSON etc...
 * 
 * a cache mechanism is implemented - so we don't parse the same datasource more than 1 time
 * 
 * usage:  DataSource.createDataSource(vqlString, databaseName)
 *
 */
public class DataSource {
	// type will be JDBC, ODBC, DF etc...
	private String type;
	private String name;
	private String folder;
	private String route;
	private String connection;
	private String header;
	private String fileNamePattern;
	private String delimiter;
	private String vql;
	private List<String> unhandled;
	private String httpAction;
	private String url;
	private String className;
	
	/**
	 * cache for storing all datasources // refactor to a generic cache class?
	 */
	private static Map<String, DataSource> cache = new HashMap<String, DataSource>();
	
	/**
	 * create a new datasource - only if it is not created already
	 * @param datasourceDef
	 * @param inDatabase
	 * @return
	 */
	static DataSource createDataSource(String datasourceDef, String inDatabase) {
		DataSource ds = new DataSource();
//		System.out.println("raw ds=" + ds);
		
		ds = ds.parse(datasourceDef, inDatabase);
//		System.out.println("cooked ds=" + ds);
		
		return ds;
		
	}
	
	private DataSource() {
		// no-arg constructor
		unhandled = new ArrayList<String>();
	}
	
	// getters - for extracting contents from the outside
	String getType() {
		return type;
	}

	String getName() {
		return name;
	}

	String getFolder() {
		return folder;
	}

	String getRoute() {
		return route;
	}

	String getConnection() {
		return connection;
	}

	String getFileNamePattern() {
		return fileNamePattern;
	}

	String getDelimiter() {
		return delimiter;
	}

	String getHeader() {
		return header;
	}

	String getHttpAction() {
		return httpAction;
	}

	String getUrl() {
		return url;
	}

	String getVql() {
		return vql;
	}
	
	String getClassName() {
		return className;
	}
	
	static DataSource getDataSource(String key) {
		return cache.get(key);
	}
	
	/**
	 * parse the vql for the datasource - lookup if the object exists, otherwise store all values we know about
	 * @param datasourceDef -VQL for the datasource
	 * @param inDatabase - the database that the datasource is createed in - for the cache key
	 * @return self if a anew object, or the cached datasource if already existing
	 */
	private DataSource parse(String datasourceDef, String inDatabase) {
		// if the object already exits
		// get the datasource type
		this.vql = datasourceDef;
		// extract the datasource details
		// split by newline
		String[] lines =  datasourceDef.split("\n");
		for (String aLine: lines) {
//			System.out.println("line=" + aLine);
			if (aLine.startsWith("CREATE DATASOURCE")) {
				String dsPattern = "CREATE\\s(?:OR REPLACE\\s)?DATASOURCE\\s(\\w+)\\s\\\"?(.*)\\\"?\\b";
				Pattern regex = Pattern.compile(dsPattern);				                                
				Matcher regexMatcher = regex.matcher(aLine);
				if (regexMatcher.find() && regexMatcher.groupCount() ==2) {
					this.type = regexMatcher.group(1);
					this.name = regexMatcher.group(2);
					if (cache.containsKey(inDatabase +"." + this.name)) {
						System.out.println("cached entry for datasource: " + inDatabase +"." + this.name);
						return cache.get(inDatabase +"." + this.name);
					}

				} else {
					System.out.println("Error:  unable to extract connection name from: " + aLine + " groupcount != 2, using regex=" + dsPattern);
				}

				// end of CREATE DATASOURCE
			} else if (aLine.trim().startsWith("FOLDER = ")) {
				String[] tokens = aLine.trim().substring(10).trim().split("'");
				if (tokens.length>0) {
					this.folder = tokens[0];  
				}
				// end of FOLDER =
			} else if (aLine.trim().startsWith("ROUTE")) {
				String[] tokens = aLine.trim().substring(6).trim().split("'");
				this.route = tokens[0].trim();
				if (tokens.length>=4) {
					if (tokens[0].trim().equals("LOCAL")) {
//						this.route = tokens[0].trim();
						this.connection = tokens[1].trim();
						this.folder = tokens[3].trim();
						if (tokens.length>5) {
							this.fileNamePattern = tokens[5].trim();
						} else {
							// no filename - it is in the folder 
//							System.out.println("wtf!~!!");
						}
					} else if (tokens[0].trim().equals("HTTP")) {
//						System.out.println("non local route found..." + aLine);
//						if (tokens[0].trim().equals("HTTP")) {
							// get or put and the location
							if (tokens.length>=4) {
								this.httpAction = tokens[2].trim();
								this.url = tokens[3].trim();
							}
//						}
						this.route = tokens[0].trim();					
					} else if (tokens[0].trim().equals("FTP")) {
						List<String> matchList = new ArrayList<String>();
						Pattern regex = Pattern.compile("[^\\s\"']+|\"[^\"]*\"|'[^']*'");
						Matcher regexMatcher = regex.matcher(aLine);
						int filePatternLoc = 0;
						while (regexMatcher.find()) {
						    matchList.add(regexMatcher.group());
						    if (regexMatcher.group().equals("FILENAMEPATTERN")) {
						    	filePatternLoc = matchList.size()-1;
						    }
						} 
						this.url = matchList.get(3).replaceAll("'", "");
						if (filePatternLoc > 0 && matchList.size() >filePatternLoc+1) {
							this.fileNamePattern = matchList.get(filePatternLoc+2).replaceAll("'", "");
						}

//						if (tokens.length>=4) {
//							this.ftpUrl = tokens[3].trim();
//						}
						
					}
				} else {
					// could be fto
					System.out.println("\n Error: HELP!!..." + aLine);
				}
				// end of ROUTE
			} else if (aLine.trim().startsWith("COLUMNDELIMITER ")) {
				String[] tokens = aLine.trim().substring(16).trim().split("'");
//				System.out.println("tokens=" + tokens.length);
				if (tokens.length>=2) {
					this.delimiter = tokens[1];
				}
				// end of COLUMNDELIMITER
			} else if (aLine.trim().startsWith("HEADER =")) {
				String[] tokens = aLine.trim().substring(9).trim().split("\\n|;");
//				System.out.println("tokens=" + tokens.length);
				if (tokens.length==1) {
					this.header = tokens[0];
				}
				// end of HEADER
			} else if (aLine.trim().startsWith("CLASSNAME=")) {
				Pattern regex = Pattern.compile("CLASSNAME=\\'(.*)\\'");
				Matcher regexMatcher = regex.matcher(aLine);
				if (regexMatcher.find() && regexMatcher.groupCount() ==1) {
					this.className = regexMatcher.group(1);					
				}

			} else {
				// un-handled entry (this is ok - but probably good to log it in debug mode
				unhandled.add(aLine);
			}
		}

		// add the object to the cache
		DataSource.cache.put(inDatabase + "." + this.name, this);
//		System.out.println("returning datasource with " + this.name + " type=" + this.type + " in " + inDatabase + " with:" + this.unhandled.size() + " unhandled parms" );
		return this;
		
	}
	

}
