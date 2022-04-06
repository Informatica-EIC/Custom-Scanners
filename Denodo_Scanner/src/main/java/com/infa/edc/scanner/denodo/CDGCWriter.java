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

    protected String RELATIONAL_PACKAGE = "com.infa.odin.models.relational";
    protected String DB_FILENAME = RELATIONAL_PACKAGE + ".Database.csv";
    protected String SCHEMA_FILENAME = RELATIONAL_PACKAGE + ".Schema.csv";
    protected String TABLE_FILENAME = RELATIONAL_PACKAGE + ".Table.csv";
    protected String TABLECOL_FILENAME = RELATIONAL_PACKAGE + ".Column.csv";
    protected String VIEW_FILENAME = RELATIONAL_PACKAGE + ".View.csv";
    protected String VIEWCOL_FILENAME = RELATIONAL_PACKAGE + ".ViewColumn.csv";
    protected String LINKS_FILENAME = "links.csv";

    static Boolean APPLY_QUOTES_TO_ALL = false;

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
        srcFiles.add(outFolder + "/" + VIEW_FILENAME);
        srcFiles.add(outFolder + "/" + VIEWCOL_FILENAME);

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
}
