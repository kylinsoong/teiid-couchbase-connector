/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.couchbase;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.OBJECT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.couchbase.CouchbaseProperties.WAVE;
import static org.teiid.translator.couchbase.CouchbaseProperties.COLON;
import static org.teiid.translator.couchbase.CouchbaseProperties.SOURCE_SEPARATOR;
import static org.teiid.translator.couchbase.CouchbaseProperties.PLACEHOLDER;
import static org.teiid.translator.couchbase.CouchbaseProperties.UNDERSCORE;
import static org.teiid.translator.couchbase.CouchbaseProperties.NAME;
import static org.teiid.translator.couchbase.CouchbaseProperties.PK;
import static org.teiid.translator.couchbase.CouchbaseProperties.DOCUMENTID;
import static org.teiid.translator.couchbase.CouchbaseProperties.DEFAULT_NAMESPACE;
import static org.teiid.translator.couchbase.CouchbaseProperties.TRUE_VALUE;
import static org.teiid.translator.couchbase.CouchbaseProperties.FALSE_VALUE;
import static org.teiid.translator.couchbase.CouchbaseProperties.IDX_SUFFIX;
import static org.teiid.translator.couchbase.CouchbaseProperties.DIM_SUFFIX;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETDOCUMENTS;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETMETADATADOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETTEXTDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETTEXTDOCUMENTS;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETTEXTMETADATADOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.SAVEDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.DELETEDOCUMENT;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.TranslatorProperty.PropertyType;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseMetadataProcessor implements MetadataProcessor<CouchbaseConnection> {
    
    public static final String IS_TOP_TABLE = MetadataFactory.COUCHBASE_URI + "ISTOPTABLE"; //$NON-NLS-1$
    public static final String IS_ARRAY_TABLE = MetadataFactory.COUCHBASE_URI + "ISARRAYTABLE"; //$NON-NLS-1$
    public static final String ARRAY_TABLE_GROUP = MetadataFactory.COUCHBASE_URI + "ARRAYTABLEGROUP"; //$NON-NLS-1$
    
    public static final String TRUE = "true"; //$NON-NLS-1$
    public static final String FALSE = "false"; //$NON-NLS-1$
    
//    public static final String PK = "PK"; //$NON-NLS-1$
    public static final String DOCUMENT_ID = "documentId"; //$NON-NLS-1$
    
    public static final String ID = "id"; //$NON-NLS-1$ 
    public static final String RESULT = "result"; //$NON-NLS-1$ 
    
    private int sampleSize = 100;
    
    private String typeNameList; //$NON-NLS-1$
    
    private Map<String, String> typeNameMap;
    
    private Map<String, Table> tableValueMap  = new HashMap<>();
            
    @Override
    public void process(MetadataFactory mf, CouchbaseConnection conn) throws TranslatorException {

        List<String> keyspaces = loadKeyspaces(conn);
        for(String keyspace : keyspaces) {
            addTable(mf, conn, conn.getNamespace(), keyspace);  
        }
       
        addProcedures(mf, conn);
    }

    private List<String> loadKeyspaces(CouchbaseConnection conn) {
        
        String namespace = conn.getNamespace();
        
        boolean isValidSchema = false;
        String n1qlNamespaces = buildN1QLNamespaces();
        Iterator<N1qlQueryRow> namespaces = conn.executeQuery(n1qlNamespaces).iterator();
        while(namespaces.hasNext()) {
            JsonObject row = namespaces.next().value();
            if(row.getString(NAME).equals(namespace)){
                isValidSchema = true;
                break;
            }
        }
        if (!isValidSchema) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29010, DEFAULT_NAMESPACE));
            namespace = DEFAULT_NAMESPACE;
        }
        
        List<String> results = new ArrayList<>();
        String n1qlKeyspaces = buildN1QLKeyspaces(namespace);
        List<N1qlQueryRow> keyspaces = conn.executeQuery(n1qlKeyspaces).allRows();
        for(N1qlQueryRow row : keyspaces){
            String keyspace = row.value().getString(NAME);
            results.add(keyspace);
        }
        
        Collections.sort(results);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29011, n1qlKeyspaces, results));
        
        return results;
    }

    /**
     * Basically, a keyspace be map to a table, keyspace name is the table name, if TranslatorProperty TypeNameList defined, 
     * a keyspace may map to several tables, for example, if the TypeNameList=`default`:`type`, 
     * then the {@link CouchbaseMetadataProcessor#addTable(MetadataFactory, CouchbaseConnection, namespace, namespace)}
     * will get all distinct `type` attribute referenced values from keyspace, and use all these values as table name.
     * 
     * If multiple keyspaces has same typed value, for example, like TypeNameList=`default`:`type`,`default2`:`type`, both default and default2 
     * has document defined {"type": "Customer"}, then the default's table name is 'Customer', default2's table name is 'default2_Customer'.
     * 
     * Scan row will add columns to table or create sub-table, nested array be map to a separated table.
     * 
     * @param mf - MetadataFactory
     * @param conn - CouchbaseConnection
     * @param namespace - couchbase namespace
     * @param keyspace - couchbase  keyspace
     */
    private void addTable(MetadataFactory mf, CouchbaseConnection conn, String namespace, String keyspace) {
        
        String nameInSource = nameInSource(keyspace);
        
        String typeName = getTypeName(nameInSource);
        List<String> dataSrcTableList = new ArrayList<>();
        if(typeName != null) {
            String typeQuery = buildN1QLTypeQuery(typeName, namespace, keyspace);
            LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29003, typeQuery)); 
            List<N1qlQueryRow> rows = conn.executeQuery(typeQuery).allRows();
            
            for(N1qlQueryRow row : rows) {
                JsonObject rowJson = row.value();
                String type = trimWave(typeName);
                String value = rowJson.getString(type);
                if(value != null) {
                    dataSrcTableList.add(value);
                }
            }
        } else {
            dataSrcTableList.add(keyspace);
        }
        
        for(String name : dataSrcTableList) {
            
            String tableName = name;
            if (mf.getSchema().getTable(name) != null && !name.equals(keyspace)) { // handle multiple keyspaces has same typed table name
                tableName = keyspace + UNDERSCORE + name;
            }
            
            Table table = mf.addTable(tableName);
            table.setNameInSource(nameInSource);
            table.setSupportsUpdate(true);
            table.setProperty(IS_ARRAY_TABLE, FALSE_VALUE);
            
            mf.addColumn(DOCUMENTID, STRING, table);
            mf.addPrimaryKey("PK0", Arrays.asList(DOCUMENTID), table); //$NON-NLS-1$
            
            // scan row
            boolean hasTypeIdentifier = true;
            if(dataSrcTableList.size() == 1 && dataSrcTableList.get(0).equals(keyspace)) {
                hasTypeIdentifier = false;
            }
            String query = buildN1QLQuery(typeName, name, namespace, keyspace, getSampleSize(), hasTypeIdentifier);
            LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29003, query)); 
            Iterator<N1qlQueryRow> result = conn.executeQuery(query).iterator();
            while(result.hasNext()) {
                JsonObject row = result.next().value(); // result.next() always can not be null
                String docuemntId = row.getString(PK);
                JsonObject currentRowJson = row.getObject(keyspace);
                scanRow(keyspace, docuemntId, currentRowJson, mf, conn, table, tableName, false);
            }            
        }
    }

    private void scanRow(String keyspace, String docuemntId, JsonValue jsonValue, MetadataFactory mf, CouchbaseConnection conn, Table table, String referenceTableName, boolean isNestedType) {
        
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29013, table, keyspace, docuemntId, jsonValue));
        
        if(isObjectJsonType(jsonValue)) {
            scanObjectRow(keyspace, docuemntId, (JsonObject)jsonValue, mf, conn, table, referenceTableName, isNestedType);
        } else if (isArrayJsonType(jsonValue)) {
            scanArrayRow(keyspace, docuemntId, (JsonArray)jsonValue, mf, conn, table, referenceTableName, isNestedType);
        }
    }

    private void scanObjectRow(String keyspace, String docuemntId, JsonObject json, MetadataFactory mf, CouchbaseConnection conn, Table table, String referenceTableName, boolean isNestedType) {
        
        Set<String> names = json.getNames();
        
        for(String name : names) {
            String columnName = name;
            Object columnValue = json.get(columnName);
            String columnType = getDataType(columnValue);

            if(columnType.equals(OBJECT)) {
                JsonValue jsonValue = (JsonValue) columnValue;
                if(isObjectJsonType(columnValue)) {
                    
                } else if(isArrayJsonType(columnValue)) {
                    String tableName = table.getName() + UNDERSCORE + columnName;
                    String tableNameInSource = table.getNameInSource() + SOURCE_SEPARATOR + nameInSource(columnName);
                    Table subTable = null;
                    if (mf.getSchema().getTable(tableName) != null) {
                        subTable = mf.getSchema().getTable(tableName);
                    } else {
                        subTable = mf.addTable(tableName);
                        subTable.setNameInSource(tableNameInSource);
                        subTable.setSupportsUpdate(true);
                        subTable.setProperty(IS_ARRAY_TABLE, TRUE_VALUE);
                        mf.addColumn(DOCUMENTID, STRING, subTable);
                        mf.addForiegnKey("FK0", Arrays.asList(DOCUMENTID), referenceTableName, subTable);
                        Column idx = mf.addColumn(tableName + IDX_SUFFIX, INTEGER, subTable);
                        idx.setUpdatable(false);
                    } 
                    scanRow(keyspace, docuemntId, jsonValue, mf, conn, subTable, referenceTableName, true);
                }
            } else {
                String columnNameInSource = nameInSource(name);
                if(isNestedType) {
                    columnName = table.getName() + UNDERSCORE + columnName;
                    columnNameInSource = table.getNameInSource() + SOURCE_SEPARATOR + columnNameInSource;
                }
                if (table.getColumnByName(columnName) == null) {
                    Column column = mf.addColumn(columnName, columnType, table);
                    column.setNameInSource(columnNameInSource);
                    column.setUpdatable(true);
                }
            }
        } 
    }

    private void scanArrayRow(String keyspace, String docuemntId, JsonArray array, MetadataFactory mf, CouchbaseConnection conn, Table table, String referenceTableName, boolean isNestedType) {
        
        if(array.size() > 0) {
            for(int i = 0 ; i < array.size() ; i ++) {
                Object element = array.get(i);
                if(isObjectJsonType(element)) {
                    
                } else if(isArrayJsonType(element)) {
                    
                } else {
                    String elementType = getDataType(element);
                    String columnName = table.getName();
                    //TODO-- here need more address
                    if (table.getColumnByName(columnName) != null) {
                        Column column = table.getColumnByName(columnName);
                        if(!column.getDatatype().getName().equals(elementType) && !column.getDatatype().getName().equals(OBJECT)) {
                            Datatype datatype = mf.getDataTypes().get(OBJECT);
                            column.setDatatype(datatype, true, 0);
                        }
                    } else {
                        Column column = mf.addColumn(columnName, elementType, table);
                        column.setUpdatable(true);
                    }
                }
            }
        } else {
            //TODO-- handle empty array []
        }
    }

    private boolean isObjectJsonType(Object jsonValue) {
        return jsonValue instanceof JsonObject;
    }

    private boolean isArrayJsonType(Object jsonValue) {
        return jsonValue instanceof JsonArray;
    }

    @Deprecated
    protected Table addTable(CouchbaseConnection connection, MetadataFactory metadataFactory, String keyspace, String sourceTableName, JsonObject value) {
        
        Table table = null;
        
        if (metadataFactory.getSchema().getTable(sourceTableName) != null) {
            table = metadataFactory.getSchema().getTable(sourceTableName);
        } else {
            table = metadataFactory.addTable(sourceTableName);
            metadataFactory.addColumn(DOCUMENTID, STRING, table);
            table.setSupportsUpdate(true);
            table.setNameInSource(buildNameInSource(keyspace, null));
            metadataFactory.addPrimaryKey("PK0", Arrays.asList(DOCUMENTID), table); //$NON-NLS-1$
            table.setProperty(IS_ARRAY_TABLE, FALSE_VALUE);
        }
        
        for(String key : value.getNames()) {
            addColumn(connection, metadataFactory, keyspace, table, null, key, value.get(key));
        }
        
        return table;
    }

    @Deprecated
    protected void addTable(CouchbaseConnection connection, MetadataFactory metadataFactory, String key, JsonObject doc, Table parent) {

//        Table table = null;
//        String tableName = key;
//        if(parent != null) {
//            tableName = parent.getName() + LINE + key;
//        }
//        if (metadataFactory.getSchema().getTable(tableName) != null) {
//            table = metadataFactory.getSchema().getTable(tableName);
//        } else {
//            table = metadataFactory.addTable(tableName);
//            metadataFactory.addColumn(DOCUMENT_ID, STRING, table);
//            table.setSupportsUpdate(true);
//            if(parent == null) {
//                // The top Table have a unique primary key.
//                table.setNameInSource(buildNameInSource(key, null));
//                table.setProperty(IS_TOP_TABLE, TRUE);
//                metadataFactory.addPrimaryKey("PK0", Arrays.asList(DOCUMENT_ID), table); //$NON-NLS-1$
//            } else {
//                table.setNameInSource(buildNameInSource(key, parent.getNameInSource()));
//                table.setProperty(IS_TOP_TABLE, FALSE);
//                metadataFactory.addForiegnKey("FK0", Arrays.asList(DOCUMENT_ID), connection.getKeyspaceName(), table);
//            }
//            table.setProperty(IS_ARRAY_TABLE, FALSE);
//        }
//
//        // add more columns
//        for(String keyCol : doc.getNames()) {
//            addColumn(connection, metadataFactory, table, keyCol, doc.get(keyCol));
//        }
    }
    
    @Deprecated
    private void addColumn(CouchbaseConnection connection, MetadataFactory metadataFactory, String keyspace, Table table, String prefix, String key, Object value) {
        
        //TODO-- couchbase is case sensitive
        String columnName = key;
        if(prefix != null) {
            columnName = prefix + UNDERSCORE + key;
        }
        if (table.getColumnByName(columnName) != null) {            
            return ;
        }
        
        String columnType = getDataType(value);
        
        if(value instanceof JsonObject) {
            JsonObject nestedObject = (JsonObject)value;
            for(String nestedKey : nestedObject.getNames()) {
                String nestedPrefix = nestedKey;
                if(prefix != null) {
                    nestedPrefix = prefix + UNDERSCORE + key;
                }
                addColumn(connection, metadataFactory, keyspace, table, nestedPrefix, nestedKey, nestedObject.get(nestedKey));
            }
            addTable(connection, metadataFactory, key, (JsonObject)value, table);
        } else if(value instanceof JsonArray) {
            //TODO--
            String nameInSource = table.getNameInSource() + SOURCE_SEPARATOR + nameInSource(key);
            String tableName = table.getName() + UNDERSCORE + key;
            String referenceTableName = table.getName();
            addArrayTable(connection, metadataFactory, nameInSource, tableName, referenceTableName, (JsonArray)value, 0);
        } else {
            Column column = metadataFactory.addColumn(columnName, columnType, table);
            column.setUpdatable(true);
        }
    }

    @Deprecated
    private void addArrayTable(CouchbaseConnection connection, MetadataFactory metadataFactory, String nameInSource, String tableName, String referenceTableName, JsonArray array, int dimension) {

        Table table = null;
        if (metadataFactory.getSchema().getTable(tableName) != null) {
            table = metadataFactory.getSchema().getTable(tableName);
        } else {
            table = metadataFactory.addTable(tableName);
            metadataFactory.addColumn(DOCUMENTID, STRING, table);
            table.setSupportsUpdate(true);
            table.setNameInSource(nameInSource);
            metadataFactory.addForiegnKey("FK0", Arrays.asList(DOCUMENTID), referenceTableName, table);
            table.setProperty(IS_ARRAY_TABLE, TRUE_VALUE);
            
            //add array index column
            Column idx = metadataFactory.addColumn(tableName + IDX_SUFFIX, INTEGER, table);
            idx.setUpdatable(false);
            
            dimension++;
        }
        
        Iterator<Object> items = array.iterator();
        while(items.hasNext()) {
            Object item = items.next();
            if(item instanceof JsonObject) {
                
            } else if (item instanceof JsonArray) {
                
            } else {
                
            }
        }
        
    }

    /**
     * Map document-format nested JsonArray to JDBC-compatible Table:
     *  If nested array contains differently-typed elements and no elements are Json document, all elements be 
     *  map to same column with Object type.
     *  If nested array contains Json document, all keys be map to Column name, and reference value data type be
     *  map to Column data type
     *  If nested array contains other nested array, or nested array's Json document item contains nested arrays/documents
     *  these nested arrays/documents be treated as Object
     * @param metadataFactory
     * @param key
     * @param value
     * @param parent
     */
    @Deprecated
    private void addTable(CouchbaseConnection connection, MetadataFactory metadataFactory, String key, JsonArray value, Table parent) {
        
        Table table = null;
        String tableName = parent.getName() + UNDERSCORE  + key;
        if (metadataFactory.getSchema().getTable(tableName) != null) {
            table = metadataFactory.getSchema().getTable(tableName);
        } else {
            table = metadataFactory.addTable(tableName);
            metadataFactory.addColumn(DOCUMENT_ID, STRING, table);
            table.setSupportsUpdate(true);
            table.setNameInSource(buildNameInSource(key, parent.getNameInSource()));
            table.setProperty(IS_ARRAY_TABLE, TRUE);
            table.setProperty(IS_TOP_TABLE, FALSE);
            table.setProperty(ARRAY_TABLE_GROUP, key);
            metadataFactory.addForiegnKey("FK0", Arrays.asList(DOCUMENT_ID), connection.getKeyspaceName(), table);
        }
        
        Iterator<Object> items = value.iterator();
        while(items.hasNext()) {
            Object item = items.next();
            if(item instanceof JsonObject) {
                JsonObject nestedJson = (JsonObject) item;
                for(String keyCol : nestedJson.getNames()) {
                    String arrayType = getDataType(nestedJson.get(keyCol));
                    if(table.getColumnByName(keyCol) != null) {
                        Column column = table.getColumnByName(keyCol);
                        if(!column.getDatatype().getName().equals(arrayType) && !column.getDatatype().getName().equals(OBJECT)) {
                            Datatype datatype = metadataFactory.getDataTypes().get(OBJECT);
                            column.setDatatype(datatype, true, 0);
                        }
                    } else {
                        Column column = metadataFactory.addColumn(keyCol, arrayType, table);
                        column.setUpdatable(true);
                    }
                }
            } else {
                String arrayType = getDataType(item);
                if (table.getColumnByName(key) != null) {
                    Column column = table.getColumnByName(key);
                    if(!column.getDatatype().getName().equals(arrayType) && !column.getDatatype().getName().equals(OBJECT)) {
                        Datatype datatype = metadataFactory.getDataTypes().get(OBJECT);
                        column.setDatatype(datatype, true, 0);
                    }
                } else {
                    Column column = metadataFactory.addColumn(key, arrayType, table);
                    column.setUpdatable(true);
                }
            }
        }
    }
    
    private String getTypeName(String keyspace) {
        
        if(this.typeNameList == null) {
            return null;
        }
        
        if(this.typeNameMap == null) {
            this.typeNameMap = new HashMap<>();
            try {
                Pattern typeNamePattern = Pattern.compile(CouchbaseProperties.TPYENAME_MATCHER_PATTERN);
                Matcher typeGroupMatch = typeNamePattern.matcher(typeNameList);
                while (typeGroupMatch.find()) {
                    typeNameMap.put(typeGroupMatch.group(1), typeGroupMatch.group(2));
                }
            } catch (Exception e) {
                LogManager.logError(LogConstants.CTX_CONNECTOR, e, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29012, typeNameList));
            }
        } 

        return this.typeNameMap.get(keyspace);
    }
    
    protected void addProcedures(MetadataFactory metadataFactory, CouchbaseConnection connection) {
        
        Procedure getTextDocuments = metadataFactory.addProcedure(GETTEXTDOCUMENTS);
        getTextDocuments.setAnnotation(CouchbasePlugin.Util.getString("getTextDocuments.Annotation")); //$NON-NLS-1$
        ProcedureParameter param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getTextDocuments); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getTextDocuments.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("id", TypeFacility.RUNTIME_NAMES.STRING, getTextDocuments); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, getTextDocuments); //$NON-NLS-1$
        
        Procedure getDocuments = metadataFactory.addProcedure(GETDOCUMENTS);
        getDocuments.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocuments); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.BLOB, getDocuments); //$NON-NLS-1$

        Procedure getTextDocument = metadataFactory.addProcedure(GETTEXTDOCUMENT);
        getTextDocument.setAnnotation(CouchbasePlugin.Util.getString("getTextDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getTextDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getTextDocument.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("id", TypeFacility.RUNTIME_NAMES.STRING, getTextDocument); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, getTextDocument); //$NON-NLS-1$
        
        Procedure getDocument = metadataFactory.addProcedure(GETDOCUMENT);
        getDocument.setAnnotation(CouchbasePlugin.Util.getString("getDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocument.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.BLOB, getDocument); //$NON-NLS-1$
        
        Procedure saveDocument = metadataFactory.addProcedure(SAVEDOCUMENT);
        saveDocument.setAnnotation(CouchbasePlugin.Util.getString("saveDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, saveDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("saveDocument.id.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("document", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, saveDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("saveDocument.document.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, saveDocument); //$NON-NLS-1$
        
        Procedure deleteDocument = metadataFactory.addProcedure(DELETEDOCUMENT);
        deleteDocument.setAnnotation(CouchbasePlugin.Util.getString("deleteDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, deleteDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("deleteDocument.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, deleteDocument); //$NON-NLS-1$
        
        Procedure getTextMetadataDocument = metadataFactory.addProcedure(GETTEXTMETADATADOCUMENT);
        getTextMetadataDocument.setAnnotation(CouchbasePlugin.Util.getString("getTextMetadataDocument.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, getTextMetadataDocument); //$NON-NLS-1$
        
        Procedure getMetadataDocument = metadataFactory.addProcedure(GETMETADATADOCUMENT);
        getMetadataDocument.setAnnotation(CouchbasePlugin.Util.getString("getMetadataDocument.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.BLOB, getMetadataDocument); //$NON-NLS-1$
        
    }

    /**
     * All supported type in a Couchbase JSON item:
     *   null, String, Integer, Long, Double, Boolean, 
     *   BigInteger, BigDecimal, JsonObject, JsonArray  
     * @param value
     * @return
     */
    private String getDataType(Object value) {
        
        if(value == null) {
            return TypeFacility.RUNTIME_NAMES.NULL;
        } else if (value instanceof String) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        } else if (value instanceof Integer) {
            return TypeFacility.RUNTIME_NAMES.INTEGER;
        } else if (value instanceof Long) {
            return TypeFacility.RUNTIME_NAMES.LONG;
        } else if (value instanceof Double) {
            return TypeFacility.RUNTIME_NAMES.DOUBLE;
        } else if (value instanceof Boolean) {
            return TypeFacility.RUNTIME_NAMES.BOOLEAN;
        } else if (value instanceof BigInteger) {
            return TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
        } else if (value instanceof BigDecimal) {
            return TypeFacility.RUNTIME_NAMES.BIG_DECIMAL;
        } 

        return TypeFacility.RUNTIME_NAMES.OBJECT;
    }
    
    private String buildN1QLNamespaces() {
        return "SELECT name FROM system:namespaces"; //$NON-NLS-1$
    }
    
    private String buildN1QLKeyspaces(String namespace) {
        return "SELECT name, namespace_id FROM system:keyspaces WHERE namespace_id = '" + namespace + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private String buildN1QLTypeQuery(String typeName, String namespace, String keyspace) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT DISTINCT "); //$NON-NLS-1$
        sb.append(typeName);
        sb.append(buildN1QLFrom(namespace, keyspace));
        return sb.toString();
    }
    
    private String buildN1QLQuery(String columnIdentifierName, String typedValue, String namespace, String keyspace, int sampleSize, boolean hasTypeIdentifier) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT meta(").append(WAVE); //$NON-NLS-1$
        sb.append(keyspace);
        sb.append(WAVE).append(").id as PK, "); //$NON-NLS-1$
        sb.append(WAVE).append(keyspace).append(WAVE);
        sb.append(buildN1QLFrom(namespace, keyspace));
        if(hasTypeIdentifier) {
            sb.append(" WHERE ").append(columnIdentifierName).append("='").append(typedValue).append("'");
        }
        sb.append(" LIMIT ").append(sampleSize);
        return sb.toString();
    }
    
    
    private String buildN1QLFrom(String namespace, String keyspace) {
        StringBuilder sb = new StringBuilder();
        sb.append(" FROM "); //$NON-NLS-1$
        sb.append(WAVE).append(namespace).append(WAVE);
        sb.append(COLON);
        sb.append(WAVE).append(keyspace).append(WAVE);
        return sb.toString();
    }
    
    private String trimWave(String value) {
        String results = value;
        if(results.startsWith(WAVE)) {
            results = results.substring(1);
        }
        if(results.endsWith(WAVE)) {
            results = results.substring(0, results.length() - 1);
        }
        return results;
    }
    
    private String nameInSource(String path) {
        return WAVE + path + WAVE; 
    }
    
    public static String buildNameInSource(String path) {
        return buildNameInSource(path, null);
    }
    
    public static String buildNameInSource(String path, String parentPath) {
        StringBuilder sb = new StringBuilder();
        if(parentPath != null) {
            sb.append(parentPath);
            sb.append(SOURCE_SEPARATOR);
        }
        sb.append(WAVE);
        sb.append(path);
        sb.append(WAVE);
        String nameInSource = sb.toString();
        return nameInSource;
    }
    
    public static String buildPlaceholder(int i) {
        return PLACEHOLDER + i; 
    }

    @TranslatorProperty(display = "SampleSize", category = PropertyType.IMPORT, description = "Maximum number of documents per keyspace that should be map") //$NON-NLS-1$ //$NON-NLS-2$
    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    @TranslatorProperty(display = "TypeNameList", category = PropertyType.IMPORT, description = "A comma-separate list of the attributes that the buckets use to specify document types. Each list item must be a bucket name surrounded by back quotes (`), a colon (:), and an attribute name surrounded by back quotes (`).") //$NON-NLS-1$ //$NON-NLS-2$
    public String getTypeNameList() {
        return typeNameList;
    }

    public void setTypeNameList(String typeNameList) {
        this.typeNameList = typeNameList;
    }
   
}
