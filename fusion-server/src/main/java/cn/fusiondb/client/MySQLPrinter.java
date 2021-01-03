/*
 * Copyright 2020 FusionLab, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package cn.fusiondb.client;

import cn.fusiondb.mysql.ConnectContext;
import cn.fusiondb.mysql.MysqlEofPacket;
import cn.fusiondb.mysql.MysqlSerializer;
import cn.fusiondb.mysql.PrimitiveType;
import com.facebook.presto.client.Column;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MySQLPrinter
        implements OutputPrinter
{
    private final List<String> fieldNames;
    private MysqlSerializer serializer;
    private ConnectContext context;

    public MySQLPrinter(ConnectContext context, List<String> fieldNames)
    {
        requireNonNull(context, "context is null");
        requireNonNull(fieldNames, "fieldNames is null");
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.context = context;
        this.serializer = context.getSerializer();
    }

    // if python's MysqlDb get error after sendfields, it can't catch the
    // excpetion
    // so We need to send fields after first batch arrived
    @Override
    public void writeRows(List<List<?>> rows, List<Column> columns, boolean complete) throws IOException {
        int colNum = columns.size();
        if (colNum == 0) {
            if (rows.size() > 0) {
                context.getState().setAffectRows(rows.size());
            }
            context.getState().setOk();
        } else {
            // send field
            List<PrimitiveType> returnTypes = Lists.newArrayList();
            List<String> returnColumnNames = Lists.newArrayList();

            columns.stream().map(column -> {
                returnTypes.add(getPrimitiveTypeFromNativeType(column.getType()));
                returnColumnNames.add(column.getName());
                return true;
            }).collect(Collectors.toSet());
            sendFields(returnColumnNames, returnTypes);

            // send result
            for (List<?> row : rows) {
                serializer.reset();
                Object[] rowObject = toObjects(row);
                List<String> colTypes = Lists.transform(columns, Column::getType);
                for (int colIndex = 0; colIndex < colTypes.size(); ++colIndex) {
                    Object value = rowObject[colIndex];
                    if (value != null) {
                        String type = colTypes.get(colIndex);
                        if (type.startsWith(StandardTypes.VARCHAR)
                                || type.startsWith(StandardTypes.CHAR)
                                || type.equals(StandardTypes.BIGINT)
                                || type.equals(StandardTypes.BOOLEAN)
                                || type.equals(StandardTypes.DOUBLE)
                                || type.equals(StandardTypes.REAL)
                                || type.equals(StandardTypes.INTEGER)
                                || type.equals(StandardTypes.SMALLINT)
                                || type.equals(StandardTypes.TIMESTAMP)
                                || type.equals(StandardTypes.DATE)
                                || type.equals(StandardTypes.TIME)
                                || type.equals(StandardTypes.BING_TILE)
                                || type.equals(StandardTypes.DECIMAL)
                        ) {
                            serializer.writeLenEncodedString(value.toString());
                        } else if (value.getClass().isArray()) {
                            // TODO error
                            JSONArray jsonArray = JSONArray.fromObject(value);
                            serializer.writeLenEncodedString(jsonArray.toString());
                        } else {
                            // TODO error
                            JSONObject json = JSONObject.fromObject(value);
                            serializer.writeLenEncodedString(json.toString());
                        }
                    } else {
                        serializer.writeNull();
                    }
                }
//                checkError();
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
            context.getState().setEof();
        }
    }

    private void sendFields(List<String> colNames, List<PrimitiveType> types)
            throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            serializer.writeField(colNames.get(i), types.get(i));
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    public static PrimitiveType getPrimitiveTypeFromNativeType(String colType) {
        if (colType.equalsIgnoreCase(StandardTypes.VARCHAR)) {
            return PrimitiveType.VARCHAR;
        } else if (colType.equalsIgnoreCase(StandardTypes.BOOLEAN)) {
            return PrimitiveType.BOOL;
        } else if (colType.equalsIgnoreCase(StandardTypes.DOUBLE)) {
            return PrimitiveType.DOUBLE;
        } else if (colType.equalsIgnoreCase(StandardTypes.REAL)) {
            return PrimitiveType.FLOAT;
        } else if (colType.equalsIgnoreCase(StandardTypes.INTEGER)) {
            return PrimitiveType.INT;
        } else if (colType.equalsIgnoreCase(StandardTypes.BIGINT)) {
            return PrimitiveType.LONG;
        } else if (colType.equalsIgnoreCase(StandardTypes.SMALLINT)) {
            return PrimitiveType.SHORT;
        } else if (colType.equalsIgnoreCase(StandardTypes.TIMESTAMP)) {
            return PrimitiveType.TIMESTAMP;
        } else if (colType.equalsIgnoreCase(StandardTypes.DATE)) {
            return PrimitiveType.DATE;
        } else if (colType.equalsIgnoreCase(StandardTypes.TIME)) {
            return PrimitiveType.TIME;
        } else if (colType.equalsIgnoreCase(StandardTypes.DECIMAL)) {
            return PrimitiveType.DECIMAL;
        } else if (colType.equalsIgnoreCase(StandardTypes.BING_TILE)) {
            return PrimitiveType.BING_TILE;
        } else if (colType.startsWith(StandardTypes.CHAR)) {
            return PrimitiveType.VARCHAR;
        }
        return PrimitiveType.STRING;
    }

    @Override
    public void finish()
            throws IOException
    {
    }

    private static Object[] toObjects(List<?> values)
    {
        Object[] array = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = values.get(i);
        }
        return array;
    }
}
