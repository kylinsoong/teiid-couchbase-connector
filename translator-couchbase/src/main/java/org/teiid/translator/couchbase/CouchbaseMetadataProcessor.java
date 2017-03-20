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
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DOUBLE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BOOLEAN;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.SHORT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BYTE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.LONG;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.FLOAT;
import static org.teiid.translator.couchbase.CouchbaseProperties.WAVE;
import static org.teiid.translator.couchbase.CouchbaseProperties.COLON;
import static org.teiid.translator.couchbase.CouchbaseProperties.DOT;
import static org.teiid.translator.couchbase.CouchbaseProperties.PLACEHOLDER;
import static org.teiid.translator.couchbase.CouchbaseProperties.LINE;
import static org.teiid.translator.couchbase.CouchbaseProperties.UNDERSCORE;
import static org.teiid.translator.couchbase.CouchbaseProperties.SQL_QUERYT_NAMESPACES;
import static org.teiid.translator.couchbase.CouchbaseProperties.NAME;
import static org.teiid.translator.couchbase.CouchbaseProperties.PK;
import static org.teiid.translator.couchbase.CouchbaseProperties.DOCUMENTID;
import static org.teiid.translator.couchbase.CouchbaseProperties.DEFAULT_NAMESPACE;
import static org.teiid.translator.couchbase.CouchbaseProperties.DEFAULT_TYPENAME;
import static org.teiid.translator.couchbase.CouchbaseProperties.SQL_QUERYT_KEYSPACES;
import static org.teiid.translator.couchbase.CouchbaseProperties.REPLACE_TARGET;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseMetadataProcessor implements MetadataProcessor<CouchbaseConnection> {
    
    public static final String IS_TOP_TABLE = MetadataFactory.COUCHBASE_URI + "ISTOPTABLE"; //$NON-NLS-1$
    public static final String IS_ARRAY_TABLE = MetadataFactory.COUCHBASE_URI + "ISARRAYTABLE"; //$NON-NLS-1$
    public static final String ARRAY_TABLE_GROUP = MetadataFactory.COUCHBASE_URI + "ARRAYTABLEGROUP"; //$NON-NLS-1$
    
    public static final String GETTEXTDOCUMENTS = "getTextDocuments"; //$NON-NLS-1$
    public static final String GETDOCUMENTS = "getDocuments"; //$NON-NLS-1$
    public static final String GETTEXTDOCUMENT = "getTextDocument"; //$NON-NLS-1$
    public static final String GETDOCUMENT = "getDocument"; //$NON-NLS-1$
    public static final String SAVEDOCUMENT = "saveDocument"; //$NON-NLS-1$
    public static final String DELETEDOCUMENT = "deleteDocument"; //$NON-NLS-1$
    public static final String GETMETADATADOCUMENT  = "getMetadataDocument"; //$NON-NLS-1$
    public static final String GETTEXTMETADATADOCUMENT  = "getTextMetadataDocument"; //$NON-NLS-1$
    
    public static final String TRUE = "true"; //$NON-NLS-1$
    public static final String FALSE = "false"; //$NON-NLS-1$
    
//    public static final String PK = "PK"; //$NON-NLS-1$
    public static final String DOCUMENT_ID = "documentId"; //$NON-NLS-1$
    
    public static final String ID = "id"; //$NON-NLS-1$ 
    public static final String RESULT = "result"; //$NON-NLS-1$ 
    
    private int sampleSize = 100;
    
    private String typeNameList; //$NON-NLS-1$
    
    private Map<String, String> typeNameMap;
    private Map<String, Table> tablesMap = new HashMap<>();
            
    @Override
    public void process(MetadataFactory metadataFactory, CouchbaseConnection connection) throws TranslatorException {
        
        String namespace = connection.getNamespace();
        
        boolean isValidSchema = false;
        Iterator<N1qlQueryRow> namespaces = connection.executeQuery(SQL_QUERYT_NAMESPACES).iterator();
        while(namespaces.hasNext()) {
            JsonObject row = namespaces.next().value();
            if(row.getString(NAME).equals(namespace)){
                isValidSchema = true;
                break;
            }
        }
        if (!isValidSchema) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29011, DEFAULT_NAMESPACE));
            namespace = DEFAULT_NAMESPACE;
        }
        
        // map all keyspaces under a namespaces
        List<N1qlQueryRow> keyspaces = connection.executeQuery(SQL_QUERYT_KEYSPACES.replace(REPLACE_TARGET, namespace)).allRows();
        for(N1qlQueryRow row : keyspaces){
            String keyspace = row.value().getString(NAME);
            generateTables(metadataFactory, connection, namespace, keyspace);  
        }
        
        
       
//        String keyspace = connection.getKeyspaceName();
//        
//        // Map data documents to tables
//        Iterator<N1qlQueryRow> result = connection.executeQuery(buildN1ql(keyspace)).iterator();
//        while(result.hasNext()) {
//            N1qlQueryRow row = result.next();
//            JsonObject doc = row.value().getObject(JSON);
//            addTable(connection, metadataFactory, keyspace, doc, null);       
//        }
//        
//        addProcedures(metadataFactory, connection);
        
    }

    private void generateTables(MetadataFactory metadataFactory, CouchbaseConnection connection, String namespace, String keyspace) {
        String typeName = getTypeName(buildNameInSource(keyspace));
        // If the TypeNameList define a keyspace/type mapping, the type reference value will be data source table,
        // else, only keyspace be treated as ata source table
        List<String> dataSrcTableList = new ArrayList<>();
        if(typeName != null) {
            String typeQuery = buildN1QLTypeQuery(typeName, namespace, keyspace);
            LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29003, typeQuery)); 
            List<N1qlQueryRow> rows = connection.executeQuery(typeQuery).allRows();
            if(rows != null && rows.size() == 1) {
                JsonObject rowJson = rows.get(0).value();
                String type = trimWave(typeName);
                String value = rowJson.getString(type);
                if(value == null) {
                    dataSrcTableList.add(keyspace);
                } 
            }
            
            for(N1qlQueryRow row : rows) {
                JsonObject rowJson = row.value();
                String type = trimWave(typeName);
                String value = rowJson.getString(type);
                if(value != null) {
                    dataSrcTableList.add(value);
                }
            }
        }
        
        for(String tableName : dataSrcTableList) {
            boolean hasTypeIdentifier = true;
            if(dataSrcTableList.size() == 1 && dataSrcTableList.get(0).equals(keyspace)) {
                hasTypeIdentifier = false;
            }
            String columnsQuery = buildN1QLColumnQuery(typeName, tableName, namespace, keyspace, getSampleSize(), hasTypeIdentifier);
            LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29003, columnsQuery)); 
            Iterator<N1qlQueryRow> result = connection.executeQuery(columnsQuery).iterator();
            while(result.hasNext()) {
                JsonObject row = result.next().value();
                JsonObject currentRowJson = row.getObject(keyspace);
                Table table = addTable(connection, metadataFactory, keyspace, tableName, currentRowJson);
                if(table != null) {
                    this.tablesMap.put(tableName, table);
                }
            }            
        }
    }

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
        }
        
        for(String key : value.getNames()) {
            addColumn(connection, metadataFactory, keyspace, table, null, key, value.get(key));
        }
        
        return table;
    }

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
    
    private void addColumn(CouchbaseConnection connection, MetadataFactory metadataFactory, String keyspace, Table table, String prefix, String key, Object value) {
        
        //TODO-- couchbase is case sensitive
        String colName = key;
        if(prefix != null) {
            colName = prefix + UNDERSCORE + key;
        }
        if (table.getColumnByName(colName) != null) {            
            return ;
        }
        
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
            String nameInSource = table.getNameInSource() + DOT + wave(key);
            String nameInTeiid = table.getName() + UNDERSCORE + key;
            String referenceTableName = table.getName();
            addTable(connection, metadataFactory, nameInSource, nameInTeiid, referenceTableName, (JsonArray)value, 0);
        } else {
            Column column = metadataFactory.addColumn(colName, getDataType(value), table);
            column.setUpdatable(true);
        }
    }

    private void addTable(CouchbaseConnection connection, MetadataFactory metadataFactory, String nameInSource, String tableName, String referenceTableName, JsonArray array, int deep) {

        Table table = null;
        if (metadataFactory.getSchema().getTable(tableName) != null) {
            table = metadataFactory.getSchema().getTable(tableName);
        } else {
            table = metadataFactory.addTable(tableName);
            metadataFactory.addColumn(DOCUMENTID, STRING, table);
            table.setSupportsUpdate(true);
            table.setNameInSource(nameInSource);
            metadataFactory.addForiegnKey("FK0", Arrays.asList(DOCUMENTID), referenceTableName, table);
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
    private void addTable(CouchbaseConnection connection, MetadataFactory metadataFactory, String key, JsonArray value, Table parent) {
        
        Table table = null;
        String tableName = parent.getName() + LINE  + key;
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
            return DEFAULT_TYPENAME;
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

    private String getDataType(Object value) {
                
        if (value instanceof String) {
            return STRING;
        } else if (value instanceof Integer) {
            return INTEGER;
        } else if (value instanceof Double) {
            return DOUBLE;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else if (value instanceof Short) {
            return SHORT;
        } else if (value instanceof Byte) {
            return BYTE;
        } else if (value instanceof Long) {
            return LONG;
        } else if (value instanceof Float) {
            return FLOAT;
        }
        return OBJECT;
    }
    
    private String buildN1QLColumnQuery(String columnIdentifierName, String tableName, String namespace, String keyspace, int sampleSize, boolean hasTypeIdentifier) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT meta(").append(WAVE); //$NON-NLS-1$
        sb.append(keyspace);
        sb.append(WAVE).append(").id as PK, "); //$NON-NLS-1$
        sb.append(WAVE).append(keyspace).append(WAVE);
        sb.append(buildN1QLFrom(namespace, keyspace));
        if(hasTypeIdentifier) {
            sb.append(" WHERE ").append(columnIdentifierName).append("='").append(tableName).append("'");
        }
        sb.append(" LIMIT ").append(sampleSize);
        return sb.toString();
    }
    
    private String buildN1QLTypeQuery(String typeName, String namespace, String keyspace) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT DISTINCT "); //$NON-NLS-1$
        sb.append(trimWave(typeName));
        sb.append(buildN1QLFrom(namespace, keyspace));
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
            results =results.substring(1);
        }
        if(results.endsWith(WAVE)) {
            results = value.substring(0, results.length() - 1);
        }
        return results;
    }
    
    private String wave(String path) {
        return WAVE + path + WAVE; 
    }
    
    public static String buildNameInSource(String path) {
        return buildNameInSource(path, null);
    }
    
    public static String buildNameInSource(String path, String parentPath) {
        StringBuilder sb = new StringBuilder();
        if(parentPath != null) {
            sb.append(parentPath);
            sb.append(DOT);
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
