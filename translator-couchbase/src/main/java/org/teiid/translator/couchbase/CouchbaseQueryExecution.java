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

import static org.teiid.translator.couchbase.CouchbaseProperties.PLACEHOLDER;
import static org.teiid.translator.couchbase.CouchbaseProperties.IDX_SUFFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.couchbase.CouchbaseMetadataProcessor.Dimension;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseQueryExecution extends CouchbaseExecution implements ResultSetExecution {
    
	private QueryExpression command;
	private Class<?>[] expectedTypes;
	
	private N1QLVisitor visitor;
	private Iterator<N1qlQueryRow> results;
	
	private Iterator<List<?>> arrayResults;
	
	public CouchbaseQueryExecution(
			CouchbaseExecutionFactory executionFactory,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, CouchbaseConnection connection) {
		super(executionFactory, executionContext, metadata, connection);
		this.command = command;
		this.expectedTypes = command.getColumnTypes();
	}

	@Override
	public void execute() throws TranslatorException {
		this.visitor = this.executionFactory.getN1QLVisitor();
		this.visitor.append(this.command);
		String sql = this.visitor.toString();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29001, sql));
		N1qlQueryResult queryResult = connection.executeQuery(sql);
		this.results = queryResult.iterator();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
	    
	    if (this.results != null && this.results.hasNext() && this.arrayResults == null) {
	        N1qlQueryRow queryRow = this.results.next();
	        if(queryRow != null) {
	            List<Object> row = new ArrayList<>(expectedTypes.length);
	            JsonObject json = queryRow.value();
	            
	            for(int i = 0 ; i < expectedTypes.length ; i ++){
	                String columnName = null;
	                Object value = null;
	                int cursor = i + 1;
	                
	                // column without reference, like 'select col, count(*) from table'
	                if(cursor <= this.visitor.getSelectColumns().size()){
	                    columnName = this.visitor.getSelectColumns().get(i);
	                    value = json.get(columnName); 
	                }
	                
	                // column with alias, like 'select col AS c_1 from table' 
	                if(value == null && (cursor <= this.visitor.getSelectColumnReferences().size()) && this.visitor.getSelectColumnReferences().get(i) != null) {
	                    columnName = this.visitor.getSelectColumnReferences().get(i);
	                    value = json.get(columnName);
	                }
	                
	                // column without reference and alias
	                if(value == null && json.getNames().contains(buildPlaceholder(1))) {
	                    columnName = buildPlaceholder(1);
	                    value = json.get(columnName);
	                }
	                
	                //handle json array
	                if(value == null && columnName.equals(IDX_SUFFIX) && expectedTypes[i].equals(Integer.class)) {
	                    arrayResults = new JsonArrayMatrixResult(row.get(0), this.visitor.getSelectColumns(), this.visitor.getSelectColumnReferences(), json).iterator();
	                    return nextArray();
	                }
	                
	                row.add(this.executionFactory.retrieveValue(expectedTypes[i], value));
	            }
	            return row;
	        }
	    } else if(this.arrayResults != null) {
	        return nextArray();
	    }
		return null;
	}

	private List<?> nextArray() throws DataNotAvailableException, TranslatorException {      
	    if(this.arrayResults != null && this.arrayResults.hasNext()) {
	        return this.arrayResults.next();
	    } else if (this.results != null && this.results.hasNext()) {
	        this.arrayResults = null;
	        return this.next();
	    }
        return null;
    }

    private String buildPlaceholder(int i) {
        return PLACEHOLDER + i; 
    }
    
    
    @Override
	public void close() {
	    this.results = null;
	    this.arrayResults = null;
	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}
	
	private class JsonArrayMatrixResult implements Iterable<List<?>> {
	    
	    private final Object documentId;
	    private JsonArray jsonArray;
	    
	    List<List<?>> allRow = new ArrayList<>();
	    
	    public JsonArrayMatrixResult(Object documentId, List<String> columnNames, List<String> columnReferences, JsonObject json) throws TranslatorException {
	        this.documentId = documentId;
	        this.jsonArray = findArray(json, columnNames, columnReferences);
	        buildMatrix(columnNames, columnReferences);
	    }

        private void buildMatrix(List<String> columnNames, List<String> columnReferences) throws TranslatorException {

            for(int i = 0 ; i < jsonArray.size() ; i ++){
                Object item = jsonArray.get(i);
                Object[] row = new Object[expectedTypes.length];
                row[0] = this.documentId;
                retrive(i, item, row, columnNames, columnReferences, new Dimension());
                allRow.add(Arrays.asList(row));
            }
        }

        private void retrive(int index, Object item, Object[] row, List<String> columnNames, List<String> columnReferences, Dimension dimension) throws TranslatorException {

            dimension.increment();
            
            for(int i = 0 ; i < expectedTypes.length ; i ++) {
                if(i == 0) {
                    setValue(row, this.documentId, i, columnNames);
                } else if (columnNames.get(i).endsWith(IDX_SUFFIX)) {
                    setValue(row, index, i, columnNames);
                }
            }
            
            setValue(row, index, columnNames);
            
            Class<?>[] expectedValueTypes = Arrays.copyOfRange(expectedTypes, dimension.dim() + 1, expectedTypes.length);
            List<String> columnValueNames = columnNames.subList(dimension.dim(), columnNames.size());
            List<String> columnValueReferences = columnReferences.subList(dimension.dim(), columnReferences.size());
            
            if(expectedValueTypes.length != columnValueNames.size()) {
                throw new TranslatorException(CouchbasePlugin.Event.TEIID29016, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29016, expectedTypes.length, columnNames.size(), columnNames, item));
            }
            
            if(item instanceof JsonArray) {
                
                JsonArray array = (JsonArray) item;
                List<Object> subItems = array.toList();
                for(int i = 0 ; i < subItems.size() ; i ++) {
                    Object subItem = subItems.get(i);
                 // TODO
                    retrive(i, subItem, row, columnNames, columnReferences, dimension);
                }
            } else {
                boolean isAddNoNested = true;
                boolean isJsonObject = (item instanceof JsonObject);
                JsonObject json = null;
                if(isJsonObject) {
                    json = (JsonObject) item;
                }
                for(int i = 0 ; i < expectedValueTypes.length ; i ++) {
                    if(isJsonObject) {
                        String columnName = columnValueNames.get(i);
                        Object value = json.get(columnName);
                        if(value == null && columnValueReferences.get(i) != null) {
                            value = json.get(columnValueReferences.get(i));
                        }
                        setValue(row, executionFactory.retrieveValue(expectedValueTypes[i], value), columnNames);
                    } else if(isAddNoNested && !isJsonObject){
                        setValue(row, executionFactory.retrieveValue(expectedValueTypes[i], item), columnNames);
                        isAddNoNested = false;
                    } else {
                        setValue(row, executionFactory.retrieveValue(expectedValueTypes[i], null), columnNames);
                    }
                }
            }   
        }

        private void setValue(Object[] row, Object item, List<String> columnNames) throws TranslatorException {
            boolean success = false;
            for(int i = 0 ; i < row.length ; i ++) {
                if(row[i] == null){
                    row[i] = item;
                    success = true;
                    break;
                }
            }
            if(!success) {
                throw new TranslatorException(CouchbasePlugin.Event.TEIID29015, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29015, expectedTypes.length, columnNames, item));
            }
        }
        
        private void setValue(Object[] row, Object item, int index, List<String> columnNames) throws TranslatorException {
            
            if(index >= row.length) {
                throw new TranslatorException(CouchbasePlugin.Event.TEIID29015, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29015, expectedTypes.length, columnNames, item));
            }
            
            row[index] = item;
        }

        private JsonArray findArray(JsonObject json, List<String> columnNames, List<String> columnReferences) throws TranslatorException {
            
            JsonArray result = null;
            
            for(String columnName : columnNames) {
                if(columnName != null && columnName.endsWith(IDX_SUFFIX)) {
                    continue;
                } else if(columnName != null && json.getNames().contains(columnName)) {
                    Object value = json.get(columnName);
                    if(value != null && value instanceof JsonArray) {
                        result = (JsonArray) value;
                        break;
                    }
                }
            }
            
            if(result != null) {
                return result;
            }
            
            for(String referenceName : columnReferences) {
                if(referenceName != null && json.getNames().contains(referenceName)) {
                    Object value = json.get(referenceName);
                    if(value != null && value instanceof JsonArray) {
                        result = (JsonArray) value;
                        break;
                    }
                }
            }
            
            if(result != null) {
                return result;
            }
            
            throw new TranslatorException(CouchbasePlugin.Event.TEIID29014, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29014, json, columnNames));
        }

        @Override
        public Iterator<List<?>> iterator() {
            return allRow.iterator();
        }
	    
	}
}
