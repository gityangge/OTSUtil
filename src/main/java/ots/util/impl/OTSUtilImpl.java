package ots.util.impl;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import ots.annotation.OTSChildClass;
import ots.annotation.OTSClass;
import ots.annotation.OTSColumn;
import ots.annotation.OTSPrimaryKey;
import ots.util.OTSUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

public class OTSUtilImpl implements OTSUtil {
    private SyncClient client;

    //必须注入client
    public OTSUtilImpl(SyncClient client) {
        this.client = client;
    }

    public <T> T serchByPrimaryKey(T t) throws Exception {
        Class<T> clz = (Class<T>) t.getClass();
        String tableName = checkOTSAnnotationAndReturnTableName(clz);
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        List<Column> columnList = Lists.newArrayList();
        loadOTSClass(t, primaryKeyBuilder, columnList);
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();
        return loadResult(clz, row);
    }

    public <T> List<T> serchRange(Class<T> clz, RangeRowQueryCriteria rangeRowQueryCriteria) throws Exception {
        GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
        List<T> resultList = Lists.newArrayList();
        for (Row row : getRangeResponse.getRows()) {
            T t = loadResult(clz, row);
            if (t != null) {
                resultList.add(t);
            }
        }
        return resultList;
    }

    public <T> List<T> searchRangeByIterator(Class<T> clz, RangeIteratorParameter rangeIteratorParameter)
            throws Exception {
        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);
        List<T> resultList = Lists.newArrayList();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            T t = loadResult(clz, row);
            if (t != null) {
                resultList.add(t);
            }
        }
        return resultList;
    }

    private <T> void loadOTSClass(T t, PrimaryKeyBuilder primaryKeyBuilder, List<Column> columnList) throws Exception {
        loadOTSClass(t, primaryKeyBuilder, columnList, "", null);
    }

    private <T> void loadOTSClass(T t, PrimaryKeyBuilder primaryKeyBuilder, List<Column> columnList, String domain, Integer id) throws Exception {
        Class<T> clz = (Class<T>) t.getClass();
        Field[] fields = clz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations) {
                Object value = field.get(t);
                if (value == null) {
                    continue;
                }
                if (annotation.annotationType() == OTSPrimaryKey.class) {
                    OTSPrimaryKey pk = field.getAnnotation(OTSPrimaryKey.class);
                    String keyName = getKeyName(pk.name(), field.getName(), domain, id);
                    switch (pk.type()) {
                        case STRING:
                            primaryKeyBuilder.addPrimaryKeyColumn(keyName,
                                    PrimaryKeyValue.fromString((String) value));
                            break;
                        case LONG:
                            if (value instanceof Integer) {
                                primaryKeyBuilder.addPrimaryKeyColumn(keyName,
                                        PrimaryKeyValue.fromLong((Integer) value));
                            } else {
                                primaryKeyBuilder.addPrimaryKeyColumn(keyName,
                                        PrimaryKeyValue.fromLong((Long) value));
                            }
                            break;
                        case BINARY:
                            primaryKeyBuilder.addPrimaryKeyColumn(keyName,
                                    PrimaryKeyValue.fromBinary((byte[]) value));
                            break;
                        case COLUMN:
                            primaryKeyBuilder.addPrimaryKeyColumn(keyName,
                                    PrimaryKeyValue.fromColumn((ColumnValue) value));
                            break;
                    }
                    break;
                } else if (annotation.annotationType() == OTSColumn.class) {
                    OTSColumn column = field.getAnnotation(OTSColumn.class);
                    String keyName = getKeyName(column.name(), field.getName(), domain, id);
                    switch (column.type()) {
                        case STRING:
                            columnList.add(new Column(keyName, ColumnValue.fromString((String) value)));
                            break;
                        case LONG:
                            if (value instanceof Integer) {
                                columnList.add(new Column(keyName, ColumnValue.fromLong((Integer) value)));
                            } else {
                                columnList.add(new Column(keyName, ColumnValue.fromLong((Long) value)));
                            }
                            break;
                        case BINARY:
                            columnList.add(new Column(keyName, ColumnValue.fromBinary((byte[]) value)));
                            break;
                        case DOUBLE:
                            columnList.add(new Column(keyName, ColumnValue.fromDouble((Double) value)));
                            break;
                        case BOOLEAN:
                            columnList.add(new Column(keyName, ColumnValue.fromBoolean((Boolean) value)));
                            break;
                    }
                    break;
                } else if (annotation.annotationType() == OTSChildClass.class) {
                    String keyName = getKeyName(null, field.getName(), domain, id);
                    if (value.getClass().isArray()) {
                        Object values[] = (Object[]) value;
                        for (int i = 0; i < values.length; i++) {
                            loadOTSClass(values[i], primaryKeyBuilder, columnList, keyName + ".", i);
                        }
                    }
                    loadOTSClass(value, primaryKeyBuilder, columnList, keyName + ".", 0);
                }
            }
        }
    }

    private <T> T loadResult(Class<T> clz, Row row) throws Exception {
        return loadResult(clz, row, "", null);
    }

    private <T> T loadResult(Class<T> clz, Row row, String domain, Integer id) throws Exception {
        if (row == null || row.isEmpty()) {
            return null;
        }
        T result = clz.newInstance();
        Field[] fields = clz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Class fieldType = field.getType();
            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation.annotationType() == OTSPrimaryKey.class) {
                    OTSPrimaryKey pk = field.getAnnotation(OTSPrimaryKey.class);
                    String keyName = getKeyName(pk.name(), field.getName(), domain, id);
                    if (row.getPrimaryKey().getPrimaryKeyColumnsMap().get(keyName) == null) {
                        return null;
                    }
                    switch (pk.type()) {
                        case STRING: {
                            String value = row.getPrimaryKey().getPrimaryKeyColumnsMap().get(keyName).getValue().asString();
                            field.set(result, value);
                            break;
                        }
                        case LONG: {
                            Long value = row.getPrimaryKey().getPrimaryKeyColumnsMap().get(keyName).getValue().asLong();
                            if (fieldType == Integer.class || fieldType == int.class) {
                                field.set(result, value.intValue());
                            } else {
                                field.set(result, value);
                            }
                            break;
                        }
                        case BINARY: {
                            byte[] value = row.getPrimaryKey().getPrimaryKeyColumnsMap().get(keyName).getValue().asBinary();
                            field.set(result, value);
                            break;
                        }
                    }
                    break;
                } else if (annotation.annotationType() == OTSColumn.class) {
                    OTSColumn column = field.getAnnotation(OTSColumn.class);

                    String keyName = getKeyName(column.name(), field.getName(), domain, id);

                    ColumnValue columnValue = null;
                    if (row.getColumnsMap().get(keyName) != null) {
                        columnValue = row.getColumnsMap().get(keyName).firstEntry().getValue();
                    }
                    if (columnValue == null) {
                        return null;
                    }
                    switch (column.type()) {
                        case STRING:
                            field.set(result, columnValue.asString());
                            break;
                        case LONG:
                            if (fieldType == Integer.class || fieldType == int.class) {
                                field.set(result, new Long(columnValue.asLong()).intValue());
                            } else {
                                field.set(result, columnValue.asLong());
                            }
                            break;
                        case BINARY:
                            field.set(result, columnValue.asBinary());
                            break;
                        case DOUBLE:
                            field.set(result, columnValue.asDouble());
                            break;
                        case BOOLEAN:
                            field.set(result, columnValue.asBoolean());
                            break;
                    }
                    break;
                } else if (annotation.annotationType() == OTSChildClass.class) {
                    String keyName = getKeyName("", field.getName(), domain, id);
                    if (field.getType().isArray()) {
                        Class childClz = field.getType().getComponentType();
                        int i = 0;
                        List<Object> resultBuffer = Lists.newArrayList();
                        while (true) {
                            Object c = loadResult(childClz, row, keyName + ".", i++);
                            if (c == null) {
                                break;
                            }
                            resultBuffer.add(childClz.cast(c));
                        }
                        field.set(result, listTransferArray(resultBuffer, childClz));
                    } else {
                        Object child = loadResult(field.getType(), row, keyName + ".", 0);
                        field.set(result, child);
                    }
                }
            }
        }
        return result;
    }

    private <T> T[] listTransferArray(List<Object> list, Class<T> clz) {
        if (list == null) {
            return null;
        }
        T[] array = (T[]) Array.newInstance(clz, list.size());
        for (int i = 0; i < list.size(); i++) {
            array[i] = (T) list.get(i);
        }
        return array;
    }

    private String getKeyName(String keyName, String def, String domain, Integer id) {
        if (keyName == null || StringUtils.isEmpty(keyName)) {
            keyName = def;
        }
        keyName = domain + keyName;
        if (id != null) {
            keyName = keyName + id;
        }
        return keyName;
    }

    @Override
    public <T> PutRowResponse wirte(T t) throws Exception {
        Class<T> clz = (Class<T>) t.getClass();
        String tableName = checkOTSAnnotationAndReturnTableName(clz);
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        List<Column> columnList = Lists.newArrayList();
        loadOTSClass(t, primaryKeyBuilder, columnList);
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        for (Column column : columnList) {
            rowPutChange.addColumn(column);
        }
        return client.putRow(new PutRowRequest(rowPutChange));
    }

    @Override
    public <T> PutRowResponse wirteIfAbsent(T t) throws Exception {
        Class<T> clz = (Class<T>) t.getClass();
        String tableName = checkOTSAnnotationAndReturnTableName(clz);
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        List<Column> columnList = Lists.newArrayList();
        loadOTSClass(t, primaryKeyBuilder, columnList);
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        rowPutChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
        for (Column column : columnList) {
            rowPutChange.addColumn(column);
        }
        return client.putRow(new PutRowRequest(rowPutChange));
    }

    private <T> String checkOTSAnnotationAndReturnTableName(Class<T> clz) throws Exception {
        boolean clzHasAnno = clz.isAnnotationPresent(OTSClass.class);
        if (!clzHasAnno) {
            throw new Exception("必须传入@OTSClass注解的类型");
        }
        String tableName = clz.getAnnotation(OTSClass.class).name();
        if (tableName == null || StringUtils.isEmpty(tableName)) {
            throw new Exception("缺少必要参数，name");
        }
        return tableName;
    }
}
