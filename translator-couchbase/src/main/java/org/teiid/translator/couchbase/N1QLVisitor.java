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

import static org.teiid.language.SQLConstants.Reserved.CAST;
import static org.teiid.language.SQLConstants.Reserved.CONVERT;
import static org.teiid.language.SQLConstants.Reserved.DISTINCT;
import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.HAVING;
import static org.teiid.language.SQLConstants.Reserved.LIMIT;
import static org.teiid.language.SQLConstants.Reserved.OFFSET;
import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.language.SQLConstants.Reserved.WHERE;
import static org.teiid.language.SQLConstants.Tokens.COMMA;
import static org.teiid.language.SQLConstants.Tokens.SPACE;
import static org.teiid.language.SQLConstants.Tokens.LPAREN;
import static org.teiid.language.SQLConstants.Tokens.RPAREN;
import static org.teiid.language.SQLConstants.Tokens.EQ;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETDOCUMENTS;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETMETADATADOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETTEXTDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETTEXTDOCUMENTS;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETTEXTMETADATADOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.SAVEDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.DELETEDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.WAVE;
import static org.teiid.translator.couchbase.CouchbaseProperties.ID;
import static org.teiid.translator.couchbase.CouchbaseProperties.RESULT;
import static org.teiid.translator.couchbase.CouchbaseProperties.IDX_SUFFIX;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_ARRAY_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.NAMED_TYPE_PAIR;
import static org.teiid.translator.couchbase.CouchbaseProperties.TRUE_VALUE;
import static org.teiid.translator.couchbase.CouchbaseProperties.DOCUMENTID;
import static org.teiid.translator.couchbase.CouchbaseProperties.SOURCE_SEPARATOR;
import static org.teiid.translator.couchbase.CouchbaseProperties.SQUARE_BRACKETS;
import static org.teiid.translator.couchbase.CouchbaseProperties.N1QL_COLUMN_ALIAS_PREFIX;
import static org.teiid.translator.couchbase.CouchbaseProperties.N1QL_TABLE_ALIAS_PREFIX;
import static org.teiid.translator.couchbase.CouchbaseProperties.UNDERSCORE;
import static org.teiid.translator.couchbase.CouchbaseProperties.UNNEST;
import static org.teiid.translator.couchbase.CouchbaseProperties.UNNEST_POSITION;
import static org.teiid.translator.couchbase.CouchbaseProperties.LET;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.visitor.SQLStringVisitor;

public class N1QLVisitor extends SQLStringVisitor{
    
    private CouchbaseExecutionFactory ef;
    
    private boolean recordColumnName = true;
    private List<String> selectColumns = new ArrayList<>();
    private List<String> selectColumnReferences = new ArrayList<>();
    
    private AliasGenerator columnAliasGenerator;
    private String tableAlias;
    
    private boolean isArrayTable = false;
    private List<CBColumn> unnestStack = new ArrayList<>();
    

    public N1QLVisitor(CouchbaseExecutionFactory ef) {
        this.ef = ef;
    }
    
    @Override
    public void visit(Select obj) {
        
        this.columnAliasGenerator = new AliasGenerator(N1QL_COLUMN_ALIAS_PREFIX);
        this.tableAlias = new AliasGenerator(N1QL_TABLE_ALIAS_PREFIX).generate();

        buffer.append(SELECT).append(Tokens.SPACE);
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }
        
        append(obj.getDerivedColumns());
        
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);      
            append(obj.getFrom());
        }
        
        appendLet(obj);
        
        appendWhere(obj);
            
        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }
        
        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE).append(HAVING).append(Tokens.SPACE);
            append(obj.getHaving());
        }
        
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
        
        if (!useSelectLimit() && obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }
    }
    
    private void appendLet(Select obj) {

        if(this.unnestStack.size() > 0) {
            buffer.append(SPACE).append(LET).append(SPACE);
            boolean comma = false;
            for(int i = 0 ; i < this.unnestStack.size() ; i++) {
                if (comma) {
                    buffer.append(COMMA);
                }
                comma = true;
                buffer.append(this.unnestStack.get(i).getValueReference());
            }
        }
 
    }

    private void appendWhere(Select obj) {

        List<String> typedList = getTypedList(obj.getFrom());
        if (obj.getWhere() != null && typedList.size() == 0) {
            buffer.append(SPACE).append(WHERE).append(SPACE);
            append(obj.getWhere());
        } else if (obj.getWhere() != null && typedList.size() > 0) {
            buffer.append(SPACE).append(WHERE).append(SPACE);
            append(obj.getWhere());
            buffer.append(SPACE).append(Reserved.AND).append(SPACE).append(typedList.get(0));
        } else if (obj.getWhere() == null && typedList.size() > 0) {
            buffer.append(SPACE).append(WHERE).append(SPACE).append(typedList.get(0));
        }
    }
    
    private List<String> getTypedList(List<TableReference> references) {
        
        List<String> typedName = new ArrayList<>();
        for(TableReference reference : references) {
            if(reference instanceof NamedTable) {
                NamedTable table = (NamedTable) reference;
                String namePair = table.getMetadataObject().getProperty(NAMED_TYPE_PAIR, false);
                if(namePair != null && namePair.length() > 2) {
                    if(this.tableAlias != null) {
                        typedName.add(nameInSource(tableAlias) + SOURCE_SEPARATOR + namePair);
                    } else {
                        typedName.add(namePair);
                    }
                }
            }
        }

        return typedName;
    }

    @Override
    public void visit(NamedTable obj) {
        
        String tableNameInSource = obj.getMetadataObject().getNameInSource();
        if(this.isArrayTable) {
            String baseName = tableNameInSource;
            AliasGenerator tableAliasGenerator = new AliasGenerator(N1QL_TABLE_ALIAS_PREFIX);
            String alias = tableAliasGenerator.generate();
            String newAlias;
            for(int i = this.unnestStack.size() ; i > 0 ; i --) {
                
                CBColumn column = this.unnestStack.get(i -1);
                String nameReference = column.getNameReference();
                StringBuilder letValueReference = new StringBuilder();
                letValueReference.append(buildEQ(nameReference));
                
                if(column.isPK()) {
                    letValueReference.append("META").append(LPAREN).append(this.nameInSource(alias)).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$ //$NON-NLS-2$
                    column.setValueReference(letValueReference.toString());
                    continue;
                } else if (column.isIdx()) {
                    letValueReference.append(UNNEST_POSITION);
                    letValueReference.append(LPAREN).append(nameInSource(alias)).append(RPAREN);
                    column.setValueReference(letValueReference.toString());
                    
                    newAlias = tableAliasGenerator.generate();
                    baseName = baseName.substring(0, baseName.length() - SQUARE_BRACKETS.length());
                    StringBuilder unnestBuilder = new StringBuilder();
                    unnestBuilder.append(UNNEST).append(SPACE);
                    unnestBuilder.append(this.nameInSource(newAlias));
                    if(!baseName.endsWith(SQUARE_BRACKETS)) { // the dim 1 array has a attribute name under keyspace
                        String dimArrayAttrName = baseName.substring(baseName.lastIndexOf(SOURCE_SEPARATOR) + 1, baseName.length());
                        unnestBuilder.append(SOURCE_SEPARATOR).append(dimArrayAttrName);
                    }
                    unnestBuilder.append(SPACE).append(this.nameInSource(alias));
                    column.setUnnest(unnestBuilder.toString());
                    alias = newAlias ;
                    continue;
                }
                
                letValueReference.append(this.nameInSource(alias));
                if(column.hasNestedObject()) {
                    letValueReference.append(SOURCE_SEPARATOR).append(this.nameInSource(column.getLeafName()));
                }
                column.setValueReference(letValueReference.toString());
            }
            String keyspace = baseName.substring(0, baseName.indexOf(SOURCE_SEPARATOR));
            buffer.append(keyspace);
            buffer.append(SPACE);
            buffer.append(nameInSource(alias));
            
            for(int i = 0 ; i < this.unnestStack.size() ; i++) {
                CBColumn column = this.unnestStack.get(i);
                if(column.hasUnnest()) {
                    buffer.append(SPACE);
                    buffer.append(column.getUnnest());
                }
            }
            
        } else {
            buffer.append(tableNameInSource);
            buffer.append(SPACE);
            buffer.append(nameInSource(tableAlias));
        }
    }

    private String buildEQ(String nameReference) {
        StringBuilder sb = new StringBuilder();
        sb.append(this.nameInSource(nameReference));
        sb.append(SPACE).append(EQ).append(SPACE);
        return sb.toString();
    }

    @Override
    protected void append(List<? extends LanguageObject> items) {
        super.append(items);
    }

    @Override
    public void visit(GroupBy obj) {
        recordColumnName = false;
        super.visit(obj);
        recordColumnName = true;
    }

    @Override
    public void visit(OrderBy obj) {
        recordColumnName = false;
        super.visit(obj);
        recordColumnName = true;
    }

    @Override
    public void visit(DerivedColumn obj) {
        if(recordColumnName) {
            selectColumnReferences.add(obj.getAlias());
        }
        append(obj.getExpression());
    }

    @Override
    public void visit(ColumnReference obj) {
        
        if(obj.getTable() != null) {
            
            String isArrayTable = obj.getTable().getMetadataObject().getProperty(IS_ARRAY_TABLE, false);
            
            if(isArrayTable.equals(TRUE_VALUE))  {
                
                this.isArrayTable = true;
                
                boolean isPK = false;
                boolean isIdx = false;
                String leafName = ""; //$NON-NLS-1$
                
                if(isPKColumn(obj)) {
                    isPK = true;
                } else if(isIDXColumn(obj)) {
                    isIdx = true;
                } else if(obj.getMetadataObject().getNameInSource() != null && !obj.getMetadataObject().getNameInSource().endsWith(SQUARE_BRACKETS)){
                    String nameInSource = obj.getMetadataObject().getNameInSource();
                    leafName = nameInSource.substring(nameInSource.lastIndexOf(SOURCE_SEPARATOR) + 1, nameInSource.length());
                    leafName = this.trimWave(leafName);
                }
                
                String colExpr = this.columnAliasGenerator.generate() + UNDERSCORE + obj.getName();

                unnestStack.add(new CBColumn(isPK, isIdx, colExpr, leafName));
                
                buffer.append(this.nameInSource(colExpr));
                if(recordColumnName) {
                    this.selectColumns.add(colExpr);
                }
                
            } else {
                
                if(isPKColumn(obj)) {
                    if(recordColumnName) {
                        buffer.append("META").append(LPAREN).append(nameInSource(tableAlias)).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$ //$NON-NLS-2$
                        String alias = this.columnAliasGenerator.generate();
                        buffer.append(this.nameInSource(alias));
                        selectColumns.add(alias);
                    } else {
                        buffer.append("META").append(LPAREN).append(nameInSource(tableAlias)).append(RPAREN).append(".id"); //$NON-NLS-1$ 
                    }
                    return;
                }
                
                String columnName = obj.getMetadataObject().getNameInSource();
                columnName = columnName.substring(columnName.indexOf(SOURCE_SEPARATOR) + 1, columnName.length());
                buffer.append(nameInSource(tableAlias));
                buffer.append(SOURCE_SEPARATOR);
                buffer.append(columnName);
                String alias = this.columnAliasGenerator.generate();
                buffer.append(SPACE).append(this.nameInSource(alias));
                if(recordColumnName) {
                    this.selectColumns.add(alias);
                }
            }

        } else {
            super.visit(obj);
        }
    }
    
    private boolean isIDXColumn(ColumnReference obj) {
        return obj.getName().endsWith(IDX_SUFFIX) && obj.getMetadataObject().getNameInSource() == null;
    }

    private boolean isPKColumn(ColumnReference obj) {
        return obj.getName().equals(DOCUMENTID) && obj.getMetadataObject().getNameInSource() == null;
    }

    @Override
    public void visit(Comparison obj) {
        recordColumnName = false;
        super.visit(obj);
        recordColumnName = true;
    }

    @Override
    public void visit(Function obj) {
        
        String functionName = obj.getName();
        if(functionName.equalsIgnoreCase(CONVERT) || functionName.equalsIgnoreCase(CAST)) {
            List<?> parts =  this.ef.getFunctionModifiers().get(functionName).translate(obj);
            buffer.append(parts.get(0));
            super.append(obj.getParameters().get(0));
            buffer.append(parts.get(2));
            return;
        } else if (functionName.equalsIgnoreCase(NonReserved.TRIM)){
            buffer.append(obj.getName()).append(LPAREN);
            append(obj.getParameters());
            buffer.append(RPAREN);
            return;
        } else if(functionName.equalsIgnoreCase("METAID")) { //$NON-NLS-1$
            buffer.append("META").append(LPAREN); //$NON-NLS-1$
            Literal literal = (Literal) obj.getParameters().get(0);
            String tableName = (String) literal.getValue();
            buffer.append(tableName);
            buffer.append(RPAREN).append(".id"); //$NON-NLS-1$
            return;
        }else if (this.ef.getFunctionModifiers().containsKey(functionName)) {
            List<?> parts =  this.ef.getFunctionModifiers().get(functionName).translate(obj);
            if (parts != null) {
                obj = (Function)parts.get(0);
            }
        } 
        super.visit(obj);
    }

    @Override
    public void visit(Limit limit) {
        if(limit.getRowOffset() > 0) {
            buffer.append(LIMIT).append(SPACE);
            buffer.append(limit.getRowLimit()).append(SPACE);
            buffer.append(OFFSET).append(SPACE);
            buffer.append(limit.getRowOffset());
        } else {
            super.visit(limit);
        }
    }

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public List<String> getSelectColumnReferences() {
        return selectColumnReferences;
    }

    @Override
    public void visit(Call call) {
        
        String procName = call.getProcedureName();
        String keyspace = null;
        if(procName.equalsIgnoreCase(GETTEXTDOCUMENTS) || procName.equalsIgnoreCase(GETTEXTDOCUMENT) || procName.equalsIgnoreCase(GETDOCUMENTS) || procName.equalsIgnoreCase(GETDOCUMENT) || procName.equalsIgnoreCase(SAVEDOCUMENT) || procName.equalsIgnoreCase(DELETEDOCUMENT)) {
            keyspace = (String) call.getArguments().get(1).getArgumentValue().getValue();
        } else if(procName.equalsIgnoreCase(GETTEXTMETADATADOCUMENT) || procName.equalsIgnoreCase(GETMETADATADOCUMENT)) {
            keyspace = (String) call.getArguments().get(0).getArgumentValue().getValue();
        }
                
        if(call.getProcedureName().equalsIgnoreCase(GETTEXTDOCUMENTS)) {
            appendClobN1QL(keyspace);
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETTEXTDOCUMENT)) {
            appendClobN1QL(keyspace);
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENTS)) {
            appendBlobN1QL(keyspace);
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENT)) {
            appendBlobN1QL(keyspace);
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(SAVEDOCUMENT)) {
            buffer.append("UPSERT INTO").append(SPACE); //$NON-NLS-1$
            buffer.append(nameInSource(keyspace)).append(SPACE); 
            buffer.append("(KEY, VALUE) VALUES").append(SPACE); //$NON-NLS-1$
            buffer.append(LPAREN);
            append(call.getArguments().get(0));
            buffer.append(COMMA).append(SPACE);
            append(call.getArguments().get(2));
            buffer.append(RPAREN);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(DELETEDOCUMENT)) {
            buffer.append(Reserved.DELETE).append(SPACE);
            buffer.append(Reserved.FROM).append(SPACE);
            buffer.append(nameInSource(keyspace)).append(SPACE);
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETTEXTMETADATADOCUMENT) || call.getProcedureName().equalsIgnoreCase(GETMETADATADOCUMENT)) {
            buffer.append(SELECT).append(SPACE);
            buffer.append("META").append(LPAREN);
            buffer.append(nameInSource(keyspace));
            buffer.append(RPAREN).append(SPACE); //$NON-NLS-1$
            buffer.append(Reserved.AS).append(SPACE);
            buffer.append(RESULT).append(SPACE);
            buffer.append(Reserved.FROM).append(SPACE);
            buffer.append(nameInSource(keyspace));
            return;
        } 
    }

    private void appendClobN1QL(String keyspace) {
        buffer.append(SELECT).append(SPACE);
        buffer.append("META").append(LPAREN).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$ //$NON-NLS-2$
        buffer.append(Reserved.AS).append(SPACE).append(ID); 
        buffer.append(COMMA).append(SPACE); 
        appendFromKeyspace(keyspace);
    }
    
    private void appendBlobN1QL(String keyspace) {
        buffer.append(SELECT).append(SPACE);
        appendFromKeyspace(keyspace);
    }
    
    private void appendFromKeyspace(String keyspace) {
        buffer.append(RESULT).append(SPACE); 
        buffer.append(Reserved.FROM).append(SPACE);
        buffer.append(nameInSource(keyspace)).append(SPACE);
        buffer.append(Reserved.AS).append(SPACE).append(RESULT).append(SPACE);
    }
    
    private void appendN1QLWhere(Call call) {
        buffer.append(Reserved.WHERE).append(SPACE);
        buffer.append("META").append(LPAREN).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$ //$NON-NLS-2$
        buffer.append(Reserved.LIKE).append(SPACE);
        append(call.getArguments().get(0));
    }
    
    private void appendN1QLPK(Call call) {
        buffer.append("USE PRIMARY KEYS").append(SPACE); //$NON-NLS-1$
        append(call.getArguments().get(0));
    }
    
    private String nameInSource(String path) {
        return WAVE + path + WAVE; 
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
    
    private class AliasGenerator {
        
        private final String prefix;
        
        private Integer aliasCounter;
        
        AliasGenerator(String prefix) {
            this.prefix = prefix;
            this.aliasCounter = Integer.valueOf(1);
        }
        
        public String generate() {  
            int index = this.aliasCounter.intValue(); 
            String alias = this.prefix + index;
            this.aliasCounter = Integer.valueOf(this.aliasCounter.intValue() + 1);
            return alias;
        }
    }
    
    private class CBColumn {
        
        private boolean isPK;
        private boolean isIdx;
        private String nameReference;
        private String leafName;
        private String valueReference;
        private String unnest;

        public CBColumn(boolean isPK, boolean isIdx, String nameReference, String leafName) {
            this.isPK = isPK;
            this.isIdx = isIdx;
            this.nameReference = nameReference;
            this.leafName = leafName;
        }

        public boolean isPK() {
            return isPK;
        }

        public boolean isIdx() {
            return isIdx;
        }

        public String getNameReference() {
            return nameReference;
        }
        
        public boolean hasNestedObject() {
            return this.leafName != null && this.leafName.length() > 0;
        }

        public String getLeafName() {
            return leafName;
        }

        public String getValueReference() {
            return valueReference;
        }

        public void setValueReference(String valueReference) {
            this.valueReference = valueReference;
        }
        
        boolean hasUnnest() {
            return this.unnest != null && this.unnest.length() > 0 ;
        }

        public String getUnnest() {
            return unnest;
        }

        public void setUnnest(String unnest) {
            this.unnest = unnest;
        }
    }
}
