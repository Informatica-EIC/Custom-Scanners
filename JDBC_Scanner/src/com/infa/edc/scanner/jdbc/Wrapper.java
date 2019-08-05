/**
 * 
 */
package com.infa.edc.scanner.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.opencsv.CSVWriter;

/**
 * @author dwrigley
 * 
 * this class is required to keep track of all of the Wrapper definitions
 * especially when generating links to non jdbc sources like DS, WS, XML, JSON etc...
 * 
 * usage:  Wrapper.createWrapper(vqlString, databaseName)
 *
 */
public class Wrapper {
	private String type;
	private String name;
	private String folder;
	private String schema;
	private String relation;
	private String dataSource;
	private DataSource dataSourceObj;
	private String sqlSentance;
	private String vql;
	private Map<String, String> outputSchema = new HashMap<String, String>();
	
	/**
	 * cache for storing all datasources // refactor to a generic cache class?
	 */
	private static Map<String, Wrapper> cache = new HashMap<String, Wrapper>();

	
	private Wrapper() {
		// no-arg constructor
	}
	
	/**
	 * create a new wrapper object & store everthing for reference later
	 * @param wrapperVQL
	 * @param inDatabase
	 * @param ds
	 * @return
	 */
	static Wrapper createWrapper(String wrapperVQL, String inDatabase) {
		Wrapper wr = new Wrapper();
//		wr.dataSourceObj = ds;
//		System.out.println("raw wrapper=" + wr);
		
		wr = wr.parse(wrapperVQL, inDatabase);
//		System.out.println("cooked wr=" + wr);
		
		return wr;		
	}
	
	String getType() {
		return type;
	}

	String getName() {
		return name;
	}

	String getFolder() {
		return folder;
	}

	String getDataSource() {
		return dataSource;
	}

	String getSqlSentance() {
		return sqlSentance;
	}

	String getVql() {
		return vql;
	}

	Map<String, String> getOutputSchema() {
		return outputSchema;
	}

	static Wrapper getWrapper(String key) {
		Wrapper theWrapper = cache.get(key);
		if (theWrapper==null) {
			System.out.println("\t\tERROR: wrapper is null " + key);
		}
		return theWrapper;
	}


	private Wrapper parse(String wrapperVQL, String inDatabase) {
		// if the object already exits
		// get the datasource type
		this.vql = wrapperVQL;
		// extract the datasource details
		// split by newline
		String[] lines =  wrapperVQL.split("\n");
		boolean isOutputSchema = false;
		boolean isSqlSentence = false;
		for (String aLine: lines) {
			if (aLine.startsWith("CREATE WRAPPER")) {
				String[] tokens = aLine.substring(15).trim().split(" ");
				if (tokens.length>=2) {
					this.type = tokens[0];
					this.name = tokens[1].replaceAll("\"", "");
										
					// check that the object aready exists (& return it)
					if (cache.containsKey(inDatabase +"." + this.name)) {
						System.out.println("cached entry for wrapper: " + inDatabase +"." + this.name);
						return cache.get(inDatabase +"." + this.name);
					}
				}
				// end of CREATE WRAPPER
			} else if (aLine.trim().startsWith("FOLDER = ")) {
				String[] tokens = aLine.trim().substring(10).trim().split("'");
				if (tokens.length>0) {
					this.folder = tokens[0];  
				}
				// end of FOLDER =
			} else if (aLine.trim().startsWith("DATASOURCENAME=")) {
				this.dataSource = aLine.trim().substring(15).replaceAll("\"", "");
				// find the datasource
				String dsKey = this.dataSource;
				if (!this.dataSource.contains(".")) {
					dsKey = inDatabase + "." +  this.dataSource;
				}
				
				this.dataSourceObj = DataSource.getDataSource(dsKey);
				if (this.dataSourceObj==null) {
					System.out.println("ERROR:  cant lookup datasource for wrapper: " + dsKey);
				}
//				System.out.println("wrapper - ds name=" + theWrapper.getDataSource() + " actual source=" + actualDS);

				// end of FOLDER =
			} else if (aLine.trim().startsWith("SQLSENTENCE=")) {
				this.sqlSentance = aLine.trim().substring(13);
				isSqlSentence = true;
				if (this.sqlSentance.endsWith(");")) {
					isSqlSentence = false;
				}
				// note - SQLSENTENCE can be split across multiple lines
			} else if (aLine.trim().startsWith("SCHEMANAME=")) {
				String[] tokens = aLine.trim().substring(12).trim().split("'");
				if (tokens.length>0) {
					this.schema = tokens[0];  
				}
//				this.relation = aLine.trim().substring(11).replaceAll("'", "");
				// end of FOLDER =
			} else if (aLine.trim().startsWith("RELATIONNAME=")) {
				this.relation = aLine.trim().substring(13).replaceAll("'", "").replaceAll("\"", "").replace(" ESCAPE", "");
				// end of FOLDER =
			} else if (aLine.trim().startsWith("OUTPUTSCHEMA (")) {
				isSqlSentence = false;
				isOutputSchema = true;
				// end of FOLDER =
			} else if (isOutputSchema) {
				// add an entry
				if (aLine.trim().equals(");")) {
					isOutputSchema = false;
				} else {
					List<String> matchList = new ArrayList<String>();
					Pattern regex = Pattern.compile("[^\\s\"']+|\"[^\"]*\"|'[^']*'");
					Matcher regexMatcher = regex.matcher(aLine);
					while (regexMatcher.find()) {
					    matchList.add(regexMatcher.group());
					} 
//					System.out.println(matchList);
					if (matchList.size()>=3) {
						this.outputSchema.put(matchList.get(0).replaceAll("\"", "").replaceAll("\"", ""), matchList.get(2).trim().replaceAll("\"", "").replaceAll("\"", ""));
						
					}

				}
			} else if (isSqlSentence) {
				// append any sqlsentance lines
				if (aLine.trim().endsWith(";'")) {
					// strip off the '
					sqlSentance = sqlSentance + "\n" + aLine.trim().substring(0, aLine.trim().length() -1 );
					isSqlSentence = false;
				} else {
					sqlSentance = sqlSentance + "\n" + aLine.trim();
				}
			}

		}

		
		Wrapper.cache.put(inDatabase + "." + this.name, this);
//		System.out.println("returning wrapper with " + this.name + " type=" + this.type + " in " + inDatabase + " with:" + this.outputSchema.size() + " fields" );
		return this;
	}

	
	protected int writeLineage(CSVWriter lineageWriter, String toDB, String toSch, String toTab, List<String> columns, boolean exportCustLineageInScanner) {
		int custLineageCount = 0;
		String connectionName = toSch + "." + dataSourceObj.getName() + ":" + dataSourceObj.getType();

		if (this.type.equals("JDBC") | this.type.equals("ODBC")) {
			
			if (this.sqlSentance != null) {
				// skip??
				System.out.print(" WARNING: sqlsentance found... no custom lineage created. " + this.sqlSentance.length() + " characters ");
				return custLineageCount;
			}
			custLineageCount++;
						
			String fromKey = this.schema + "/" + this.relation;
			String toKey = "$etlRes://" +  toDB + "/" +toSch + "/" + toTab;
			if (! exportCustLineageInScanner) {
				toKey = toSch + "/" + toTab;
			} else {
				// custom linege in the scanner needs table level lineage too (external lineage does not)
				lineageWriter.writeNext(new String[] {"core.DataSetDataFlow",connectionName.replaceAll("\"", ""), 
	    				"",
	    				this.schema + "/" + this.relation,
	    				"$etlRes://" +  toDB + "/" + toSch + "/" + toTab,
	    				""});
			}
			
			for (String tgtCol : columns) {
				String fromCol = this.outputSchema.get(tgtCol);
				if (fromCol==null || fromCol.isEmpty()) {
					System.out.println("\tERROR: no from col mapped???? to=" + toSch + "." + toTab + "." + tgtCol);
					System.out.println(this.outputSchema.keySet());
				} else {
					custLineageCount++;
					if (exportCustLineageInScanner) {
						lineageWriter.writeNext(new String[] {"core.DirectionalDataFlow",connectionName, 
			    				"",  // to connection
			    				fromKey + "/" + this.outputSchema.get(tgtCol),
			    				toKey + "/" + tgtCol,
			    				""});
					} else {
						// re
						lineageWriter.writeNext(new String[] {"core.DirectionalDataFlow",connectionName, 
			    				"",  // to connection
			    				fromKey + "/" + this.outputSchema.get(tgtCol),
			    				toKey + "/" + tgtCol
			    				} );
					}
				}
				
			}
			
		} else if (this.type.equals("DF")) {
			if (this.dataSourceObj==null) {
				System.out.println("ERROR: null datasource???");
			} else {
				if (this.dataSourceObj.getRoute()==null) {
					System.out.println("ERROR: null route in datasource???");
					
				}
			}
			if (this.dataSourceObj.getRoute().equals("LOCAL") ) {
				String folder = dataSourceObj.getFolder();
				if (folder.startsWith("/")) {
					folder = folder.substring(1);
				}
				custLineageCount++;
				String fromKey = (folder + "/" + dataSourceObj.getFileNamePattern()).replaceAll("//", "/");
				String toKey = "$etlRes://" +  toDB + "/" +toSch + "/" + toTab;
				if (! exportCustLineageInScanner) {
					toKey = toSch + "/" + toTab;
				} else {
					lineageWriter.writeNext(new String[] {"core.DataSetDataFlow",connectionName, 
		    				"",  // to connection
		    				fromKey,
		    				toKey,
		    				""});
				}
				
				for (String tgtCol : columns) {
					String fromCol = this.outputSchema.get(tgtCol);
					if (fromCol==null || fromCol.isEmpty()) {
						System.out.println("ERROR: no from col mapped???? to=" + toSch + "." + toTab + "." + tgtCol);
					} else {
						custLineageCount++;
						lineageWriter.writeNext(new String[] {"core.DirectionalDataFlow",connectionName, 
			    				"",  // to connection
			    				fromKey + "/" + this.outputSchema.get(tgtCol),
			    				toKey + "/" + tgtCol,
			    				""});
					}
					
				}
					
			} else {
				// non local - e.g. HTTP - not sure what to do with this?  create a file????
				System.out.print("\t\tnon local df route " + dataSourceObj.getRoute() + " - no linage (yet)");
				System.out.print("\n\t\tmay need to create a file object?? " + dataSourceObj.getRoute() + " url=" + dataSourceObj.getUrl() + " fileNamePattern=" + dataSourceObj.getFileNamePattern());
			}
			
		} else {
			System.out.print("\twrapper type:" + this.type + " not yet supported - or proxy object(s) need to be created. ");	
		}

		return custLineageCount;
	}

}
