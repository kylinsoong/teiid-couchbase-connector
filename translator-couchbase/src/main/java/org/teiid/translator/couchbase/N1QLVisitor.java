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
import static org.teiid.translator.couchbase.CouchbaseProperties.PK;
import static org.teiid.translator.couchbase.CouchbaseProperties.IDX_SUFFIX;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_ARRAY_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.NAMED_TYPE_PAIR;
import static org.teiid.translator.couchbase.CouchbaseProperties.TRUE_VALUE;
import static org.teiid.translator.couchbase.CouchbaseProperties.DOCUMENTID;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
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
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.visitor.SQLStringVisitor;

public class N1QLVisitor extends SQLStringVisitor{
    
    private CouchbaseExecutionFactory ef;
    
    private boolean recordColumnName = true;
    private boolean isNestedArrayColumns = false;
    private List<String> selectColumns = new ArrayList<>();
    private List<String> selectColumnReferences = new ArrayList<>();

    public N1QLVisitor(CouchbaseExecutionFactory ef) {
        this.ef = ef;
    }

    @Override
    protected void append(List<? extends LanguageObject> items) {
        
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                if(!isNestedArrayColumns) {
                    buffer.append(COMMA).append(SPACE);
                }
                append(items.get(i));
            }
        }
        isNestedArrayColumns = false;
    }

    @Override
    public void visit(Select obj) {
        if (obj.getWith() != null) {
            append(obj.getWith());
        }
        buffer.append(SELECT).append(Tokens.SPACE);
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }
        append(obj.getDerivedColumns());
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);      
            append(obj.getFrom());
        }
        
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

    private List<String> getTypedList(List<TableReference> references) {
        
        List<String> typedName = new ArrayList<>();
        for(TableReference reference : references) {
            if(reference instanceof NamedTable) {
                NamedTable table = (NamedTable) reference;
                String namePair = table.getMetadataObject().getProperty(NAMED_TYPE_PAIR, false);
                typedName.add(namePair);
            }
        }

        return typedName;
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
        super.visit(obj);
    }

    @Override
    public void visit(ColumnReference obj) {
        
        NamedTable groupTable = obj.getTable();
        if(groupTable != null) {
//            String group = obj.getTable().getCorrelationName();
            String isArrayTable = obj.getTable().getMetadataObject().getProperty(IS_ARRAY_TABLE, false);
      
            
            if(obj.getName().equals(DOCUMENTID)) {
                if(recordColumnName) {
                    buffer.append("META().id").append(SPACE).append(Reserved.AS).append(SPACE).append(PK); //$NON-NLS-1$ 
                    selectColumns.add(PK);
                } else {
                    buffer.append("META().id"); //$NON-NLS-1$ 
                }
                return;
            }
            
            System.out.println(obj);
            System.out.println(obj.getName());
            
            if(isArrayTable.equals(TRUE_VALUE)) { // handle array
                if(obj.getName().endsWith(IDX_SUFFIX)) {
                    return;
                }
                
            } else {
                String columnName = nameInSource(obj.getName());
                buffer.append(columnName);
            }
            
//            if(isArrayTable.equals(TRUE_VALUE) && !isNestedArrayColumns){
//                if(group == null) {
//                    group = obj.getTable().getMetadataObject().getProperty(ARRAY_TABLE_GROUP, false);
//                }
//                buffer.append(group);
//                selectColumns.add(group);
//                isNestedArrayColumns = true;
//            } else if(isArrayTable.equals(FALSE_VALUE) && isTopTable.equals(FALSE_VALUE) && group == null) {
//                shortNameOnly = true;
//                super.visit(obj);
//                shortNameOnly = false;
//            } else if(isArrayTable.equals(FALSE_VALUE)){
//                super.visit(obj);
//            }
//            
            //add selectColumns
            if(recordColumnName){
                selectColumns.add(obj.getName());
            }
        } else {
            super.visit(obj);
        }
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
    
    private String keySpace;

    public String getKeySpace() {
        return keySpace;
    }

    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    @Override
    public void visit(Call call) {
                
        if(call.getProcedureName().equalsIgnoreCase(GETTEXTDOCUMENTS)) {
            appendClobN1QL();
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETTEXTDOCUMENT)) {
            appendClobN1QL();
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENTS)) {
            appendBlobN1QL();
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENT)) {
            appendBlobN1QL();
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(SAVEDOCUMENT)) {
            buffer.append("UPSERT INTO").append(SPACE); //$NON-NLS-1$
            buffer.append(nameInSource(keySpace)).append(SPACE);
            buffer.append(Reserved.AS).append(SPACE);
            buffer.append(RESULT).append(SPACE); 
            buffer.append("(KEY, VALUE) VALUES").append(SPACE); //$NON-NLS-1$
            buffer.append(LPAREN);
            final List<Argument> params = call.getArguments();
            for (int i = 0; i < params.size(); i++) {
                Argument param = params.get(i);
                if (param.getDirection() == Direction.IN ) {
                    if (i != 0) {
                        buffer.append(COMMA).append(SPACE);
                    }
                    append(param);
                }
            }
            buffer.append(RPAREN);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(DELETEDOCUMENT)) {
            buffer.append(Reserved.DELETE).append(SPACE);
            buffer.append(Reserved.FROM).append(SPACE);
            buffer.append(nameInSource(keySpace)).append(SPACE);
            buffer.append(Reserved.AS).append(SPACE);
            buffer.append(RESULT).append(SPACE); 
            appendN1QLPK(call);
            buffer.append(SPACE);
            buffer.append("RETURNING").append(SPACE); //$NON-NLS-1$
            buffer.append(RESULT);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETTEXTMETADATADOCUMENT) || call.getProcedureName().equalsIgnoreCase(GETMETADATADOCUMENT)) {
            buffer.append(SELECT).append(SPACE);
            buffer.append("META").append(LPAREN).append(RPAREN).append(SPACE); //$NON-NLS-1$
            buffer.append(Reserved.AS).append(SPACE);
            buffer.append(RESULT).append(SPACE);
            buffer.append(Reserved.FROM).append(SPACE);
            buffer.append(nameInSource(keySpace));
            return;
        } 
    }

    private void appendClobN1QL() {
        buffer.append(SELECT).append(SPACE);
        buffer.append("META").append(LPAREN).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$
        buffer.append(Reserved.AS).append(SPACE).append(ID); //$NON-NLS-1$
        buffer.append(COMMA).append(SPACE); 
        buffer.append(RESULT).append(SPACE); 
        buffer.append(Reserved.FROM).append(SPACE);
        buffer.append(nameInSource(keySpace)).append(SPACE);
        buffer.append(Reserved.AS).append(SPACE).append(RESULT).append(SPACE);
    }
    
    private void appendBlobN1QL() {
        buffer.append(SELECT).append(SPACE);
        buffer.append(RESULT).append(SPACE); 
        buffer.append(Reserved.FROM).append(SPACE);
        buffer.append(nameInSource(keySpace)).append(SPACE);
        buffer.append(Reserved.AS).append(SPACE).append(RESULT).append(SPACE);
    }
    
    private void appendN1QLWhere(Call call) {
        buffer.append(Reserved.WHERE).append(SPACE);
        buffer.append("META").append(LPAREN).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$
        buffer.append(Reserved.LIKE).append(SPACE);
        append(call.getArguments().get(0));
    }
    
    private void appendN1QLPK(Call call) {
        buffer.append("USE PRIMARY KEYS").append(SPACE);
        append(call.getArguments().get(0));
    }
    
    private String nameInSource(String path) {
        return WAVE + path + WAVE; 
    }
}
