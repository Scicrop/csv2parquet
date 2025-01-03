package com.scicrop.infinitestack;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Map;

public class SchemaGenerator {

    /**
     * Generates an Avro schema based on a map of headers and types.
     *
     * @param fieldMap A map where keys are column names and values are types (String, Integer, etc.).
     * @return Generated Avro schema.
     */
    public static Schema getSchemaByMap(Map<String, Class<?>> fieldMap) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("DynamicRecord");
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        for (Map.Entry<String, Class<?>> entry : fieldMap.entrySet()) {
            String fieldName = entry.getKey();
            Class<?> fieldType = entry.getValue();

            // Map Java types to Avro types
            if (fieldType == String.class) {
                fieldAssembler.name(fieldName).type().nullable().stringType().noDefault();
            } else if (fieldType == Integer.class) {
                fieldAssembler.name(fieldName).type().nullable().intType().noDefault();
            } else if (fieldType == Double.class) {
                fieldAssembler.name(fieldName).type().nullable().doubleType().noDefault();
            } else if (fieldType == Float.class) {
                fieldAssembler.name(fieldName).type().nullable().floatType().noDefault();
            } else if (fieldType == Boolean.class) {
                fieldAssembler.name(fieldName).type().nullable().booleanType().noDefault();
            } else {
                throw new IllegalArgumentException("Unsupported type: " + fieldType.getSimpleName());
            }
        }

        return fieldAssembler.endRecord();
    }

    /**
     * Example usage of the method.
     */
    public static void main(String[] args) {
        Map<String, Class<?>> fieldMap = Map.of(
                "InvoiceNo", String.class,
                "StockCode", String.class,
                "Description", String.class,
                "Quantity", Integer.class,
                "InvoiceDate", String.class,
                "UnitPrice", Double.class,
                "CustomerID", String.class,
                "Country", String.class
        );

        Schema schema = getSchemaByMap(fieldMap);
        System.out.println(schema.toString(true));
    }
}
