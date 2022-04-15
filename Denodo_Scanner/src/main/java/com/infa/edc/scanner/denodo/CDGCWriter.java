package com.infa.edc.scanner.denodo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
// import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.opencsv.CSVWriter;

/**
 * writer class to format CDGC/Hawk metadata for custom import with lineage
 */
public class CDGCWriter {

    protected String outFolder;
    // file variables
    protected CSVWriter dbWriter = null;
    protected CSVWriter schemaWriter = null;
    protected CSVWriter tableWriter = null;
    protected CSVWriter viewWriter = null;
    protected CSVWriter columnWriter = null;
    protected CSVWriter viewColumnWriter = null;
    protected CSVWriter linksWriter = null;
    protected CSVWriter refDataSourceWriter = null;
    protected CSVWriter refDataSetWriter = null;
    protected CSVWriter refDataElementWriter = null;
    protected CSVWriter refResourceWriter = null;

    protected String RELATIONAL_PACKAGE = "com.infa.odin.models.relational";
    protected String DB_FILENAME = RELATIONAL_PACKAGE + ".Database.csv";
    protected String SCHEMA_FILENAME = RELATIONAL_PACKAGE + ".Schema.csv";
    protected String TABLE_FILENAME = RELATIONAL_PACKAGE + ".Table.csv";
    protected String TABLECOL_FILENAME = RELATIONAL_PACKAGE + ".Column.csv";
    protected String VIEW_FILENAME = RELATIONAL_PACKAGE + ".View.csv";
    protected String VIEWCOL_FILENAME = RELATIONAL_PACKAGE + ".ViewColumn.csv";
    protected String LINKS_FILENAME = "links.csv";

    protected String REF_RESOURCE_FILENAME = "core.Resource.csv";
    protected String REF_DATASOURCE_FILENAME = "core.DataSource.csv";
    protected String REF_DATASET_FILENAME = "core.DataSet.csv";
    protected String REF_DATAELEMENT_FILENAME = "core.DataElement.csv";

    static Boolean APPLY_QUOTES_TO_ALL = false;

    private List<String> ref_resources = new ArrayList<String>();
    private List<String> ref_datasources = new ArrayList<String>();

    protected CDGCWriter() {

    }

    protected boolean initFiles(String folder) {
        this.outFolder = folder;
        // don't call super.initfiles() - we have more headers for tables for denodo
        boolean initialized = true;
        System.out.println("Step 3.a: initializing CDGC files in: " + this.outFolder);

        try {
            // check that the folder exists - if not, create it
            File directory = new File(String.valueOf(this.outFolder));
            if (!directory.exists()) {
                System.out.println("\tfolder: " + this.outFolder + " does not exist, creating it");
                directory.mkdir();
            }

            dbWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" + DB_FILENAME))
            // CSVWriter.DEFAULT_SEPARATOR,
            // CSVWriter.NO_QUOTE_CHARACTER,
            // CSVWriter.DEFAULT_ESCAPE_CHARACTER,
            // CSVWriter.DEFAULT_LINE_END
            );
            dbWriter.writeNext(new String[] { "core.externalId", "core.reference", "core.assignable", "core.name",
                    "core.description" }, APPLY_QUOTES_TO_ALL);

            schemaWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" + SCHEMA_FILENAME)));
            schemaWriter.writeNext(new String[] { "core.externalId", "core.reference", "core.assignable", "core.name",
                    "core.description" }, APPLY_QUOTES_TO_ALL);

            tableWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(outFolder + "/" + TABLE_FILENAME)));
            tableWriter.writeNext(new String[] { "core.externalId", "core.name", "core.reference", "core.assignable",
                    "core.description" }, APPLY_QUOTES_TO_ALL);

            columnWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(outFolder + "/" + TABLECOL_FILENAME)));
            columnWriter.writeNext(new String[] { "core.externalId", "core.name", "core.reference", "core.assignable",
                    "com.infa.odin.models.relational.Datatype", "com.infa.odin.models.relational.DatatypeLength",
                    "com.infa.odin.models.relational.Position",
                    "core.description" }, APPLY_QUOTES_TO_ALL);

            viewWriter = new CSVWriter(new BufferedWriter(new FileWriter(outFolder + "/" + VIEW_FILENAME)));
            viewWriter.writeNext(new String[] { "core.externalId", "core.name", "core.reference", "core.assignable",
                    "core.description", RELATIONAL_PACKAGE + ".sourceStatementText" }, APPLY_QUOTES_TO_ALL);

            viewColumnWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(outFolder + "/" + VIEWCOL_FILENAME)));
            viewColumnWriter.writeNext(new String[] { "core.externalId", "core.name", "core.reference",
                    "core.assignable",
                    "com.infa.odin.models.relational.Datatype", "com.infa.odin.models.relational.DatatypeLength",
                    "com.infa.odin.models.relational.Position",
                    "core.description", "com.infa.odin.models.relational.expression" }, APPLY_QUOTES_TO_ALL);

            this.linksWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" +
                            LINKS_FILENAME)));
            linksWriter.writeNext(new String[] { "Source", "Target",
                    "Association" }, APPLY_QUOTES_TO_ALL);

            refResourceWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" +
                            REF_RESOURCE_FILENAME)));
            refResourceWriter.writeNext(new String[] { "core.externalId", "core.Reference",
                    "core.assignable", "core.name" }, APPLY_QUOTES_TO_ALL);

            refDataSourceWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" +
                            REF_DATASOURCE_FILENAME)));
            refDataSourceWriter.writeNext(new String[] { "core.externalId", "core.Reference",
                    "core.assignable", "core.name" }, APPLY_QUOTES_TO_ALL);

            refDataSetWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" +
                            REF_DATASET_FILENAME)));
            refDataSetWriter.writeNext(new String[] { "core.externalId", "core.Reference",
                    "core.assignable", "core.name" }, APPLY_QUOTES_TO_ALL);

            refDataElementWriter = new CSVWriter(
                    new BufferedWriter(new FileWriter(this.outFolder + "/" +
                            REF_DATAELEMENT_FILENAME)));
            refDataElementWriter.writeNext(new String[] { "core.externalId", "core.Reference",
                    "core.assignable", "core.name" }, APPLY_QUOTES_TO_ALL);

            System.out.println("\tCDGC Files initialized");

        } catch (IOException e1) {
            initialized = false;
            e1.printStackTrace();
        }

        return initialized;

    }

    protected boolean closeFiles() {
        System.out.println("closing CDGC csv files");

        try {
            linksWriter.close();
            dbWriter.close();
            schemaWriter.close();
            tableWriter.close();
            columnWriter.close();
            viewWriter.close();
            viewColumnWriter.close();
            refDataSourceWriter.close();
            refDataSetWriter.close();
            refDataElementWriter.close();
            refResourceWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (Exception ex) {
            System.out.println("un-known exception caught" + ex.getMessage());
            ex.printStackTrace();
        }
        List<String> srcFiles = new ArrayList<String>();
        srcFiles.add(outFolder + "/" + DB_FILENAME);
        srcFiles.add(outFolder + "/" + SCHEMA_FILENAME);
        srcFiles.add(outFolder + "/" + LINKS_FILENAME);
        srcFiles.add(outFolder + "/" + TABLE_FILENAME);
        srcFiles.add(outFolder + "/" + TABLECOL_FILENAME);
        srcFiles.add(outFolder + "/" + VIEWCOL_FILENAME);
        srcFiles.add(outFolder + "/" + VIEW_FILENAME);
        srcFiles.add(outFolder + "/" + REF_RESOURCE_FILENAME);
        srcFiles.add(outFolder + "/" + REF_DATASOURCE_FILENAME);
        srcFiles.add(outFolder + "/" + REF_DATASET_FILENAME);
        srcFiles.add(outFolder + "/" + REF_DATAELEMENT_FILENAME);

        try {
            System.out.println(
                    "creating zip file: " + outFolder + "/denodo_scanner_cdgc.zip");
            FileOutputStream fos = new FileOutputStream(
                    outFolder + "/denodo_scanner_cdgc.zip");
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            for (String srcFile : srcFiles) {
                File fileToZip = new File(srcFile);
                FileInputStream fis;
                fis = new FileInputStream(fileToZip);
                ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
                zipOut.putNextEntry(zipEntry);

                byte[] bytes = new byte[1024];
                int length;
                while ((length = fis.read(bytes)) >= 0) {
                    zipOut.write(bytes, 0, length);
                }
                fis.close();
            }
            zipOut.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    protected void createDatabase(String dbName) {
        System.out.println("\tcreating database: " + dbName);

        try {
            dbWriter.writeNext(new String[] { dbName, "", "TRUE", dbName, "desc with, comma" }, APPLY_QUOTES_TO_ALL);

            // dbCount++;
            linksWriter.writeNext(new String[] { "$resource", dbName, "core.ResourceParentChild" },
                    APPLY_QUOTES_TO_ALL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    protected void createSchema(String dbName, String schema) {
        String schId = dbName + "/" + schema;

        try {
            schemaWriter.writeNext(new String[] { schId, "", "TRUE", schema, "" }, APPLY_QUOTES_TO_ALL);
            linksWriter.writeNext(new String[] { dbName, schId, RELATIONAL_PACKAGE + ".DatabaseToSchema" },
                    APPLY_QUOTES_TO_ALL);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    public void createTable(String dbName, String schema, String table, String desc,
            String location) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;

        try {
            this.tableWriter.writeNext(new String[] { tabId, table, "", "", desc }, APPLY_QUOTES_TO_ALL);
            this.linksWriter.writeNext(new String[] { schId, tabId, RELATIONAL_PACKAGE + ".SchemaToTable" },
                    APPLY_QUOTES_TO_ALL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    protected void createColumn(String dbName, String schema, String table, String column, String type, String length,
            String pos, String desc) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;
        String colId = tabId + "/" + column;

        try {
            this.columnWriter.writeNext(new String[] { colId, column, "", "", type, length, pos, desc },
                    APPLY_QUOTES_TO_ALL);
            this.linksWriter.writeNext(new String[] { tabId, colId, RELATIONAL_PACKAGE + ".TableToColumn" },
                    APPLY_QUOTES_TO_ALL);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    public void createView(String dbName, String schema, String table, String desc,
            String viewStatement, String location) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;

        try {
            this.viewWriter.writeNext(new String[] { tabId, table, "", "", desc, viewStatement.trim() },
                    APPLY_QUOTES_TO_ALL);
            this.linksWriter.writeNext(new String[] { schId, tabId, RELATIONAL_PACKAGE + ".SchemaToView" },
                    APPLY_QUOTES_TO_ALL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    protected void createViewColumn(String dbName, String schema, String table, String column, String type,
            String length,
            String pos, String desc, String expr) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;
        String colId = tabId + "/" + column;

        try {
            this.viewColumnWriter.writeNext(new String[] { colId, column, "", "", type, length, pos, desc, expr },
                    APPLY_QUOTES_TO_ALL);
            this.linksWriter.writeNext(new String[] { tabId, colId, RELATIONAL_PACKAGE + ".ViewToViewColumn" },
                    APPLY_QUOTES_TO_ALL);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    public void writeLineage(String fromId, String toId, String linkType) {
        linksWriter.writeNext(new String[] { fromId, toId, linkType }, APPLY_QUOTES_TO_ALL);
    }

    public void createDataSetDataFlow(String fromId, String toId) {
        linksWriter.writeNext(new String[] { fromId, toId, "core.DataSetDataFlow" }, APPLY_QUOTES_TO_ALL);
    }

    public void createReferenceDataset(String dbName, String schemaName, String tableName, List<String> columns,
            Wrapper wrapperObj) {
        // # this does not work
        // return
        String wrappedSchema = wrapperObj.getSchema();
        String wrappedTable = wrapperObj.getName();

        System.out.println("creating ref objects for " + dbName + "." + schemaName + "." + tableName + " with "
                + columns.size() + "columns");

        String conectionName = wrapperObj.getDataSource() + "__" + wrapperObj.getType();
        System.out.println("connection name: " + conectionName);

        String dataSourceId = conectionName + "." + wrappedSchema;
        // add schema data source only if not already there

        // create a reference resource- that contains the datasets
        if (!ref_resources.contains(conectionName)) {
            refResourceWriter.writeNext(new String[] { conectionName, "TRUE",
                    "", conectionName }, APPLY_QUOTES_TO_ALL);
            linksWriter.writeNext(new String[] { "$resource", conectionName,
                    "core.ResourceParentChild" });

            ref_resources.add(conectionName);
        }

        // create the datasource (schema), if not already created
        if (!ref_datasources.contains(dataSourceId)) {
            refDataSourceWriter.writeNext(new String[] { dataSourceId, "TRUE",
                    "TRUE", dataSourceId }, APPLY_QUOTES_TO_ALL);

            // add resource parent child for the datasource
            linksWriter.writeNext(new String[] { conectionName, dataSourceId,
                    "core.ResourceParentChild" });

            ref_datasources.add(dataSourceId);
        }
        // now look at the tables
        String dataSetId = dataSourceId + "/" + wrappedTable;
        String newdataSetId = wrappedSchema + "/" + wrappedTable;
        String linkedDataSet = dbName + "/" + schemaName + "/" + tableName;
        refDataSetWriter.writeNext(new String[] { newdataSetId, "TRUE",
                "", wrappedTable }, APPLY_QUOTES_TO_ALL);
        linksWriter
                .writeNext(new String[] { dataSourceId, newdataSetId,
                        "core.DataSourceParentChild"
                });

        linksWriter
                .writeNext(new String[] { newdataSetId, linkedDataSet, "core.DataSetDataFlow"
                });

        // contained columns...
        for (String tgtCol : columns) {
            String fromCol = wrapperObj.getOutputSchema().get(tgtCol);
            if (fromCol == null || fromCol.isEmpty()) {
                System.out.println("\tERROR: no from col mapped???? to=" + dataSetId + "." + tgtCol);
                System.out.println(wrapperObj.getOutputSchema().keySet());
            } else {
                String fromColId = newdataSetId + "/" + fromCol;

                refDataElementWriter.writeNext(new String[] { fromColId, "TRUE",
                        "", fromCol }, APPLY_QUOTES_TO_ALL);
                linksWriter
                        .writeNext(new String[] { newdataSetId, fromColId,
                                "core.DataSetToDataElementParentship"
                        });

                // custLineageCount++;
                linksWriter.writeNext(new String[] { // connection
                        fromColId, linkedDataSet + "/" + tgtCol, "core.DirectionalDataFlow" });

            }

        }

    }
}
