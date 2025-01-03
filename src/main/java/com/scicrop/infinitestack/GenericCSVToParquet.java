package com.scicrop.infinitestack;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericCSVToParquet {

    public static void main(String[] args) {
        String csvFilePath = args[0];
        String parquetFilePath = args[1];
        process(csvFilePath, parquetFilePath, args[2].charAt(0));
    }

    private static void process(String csvFilePath, String parquetFilePath, char delimiter) {
        try (
                CSVParser csvParserInfer = getCsvParser(csvFilePath, delimiter);
        ) {
            // Infer Avro schema from CSV
            Schema schema = inferSchema(csvParserInfer);

            // Configure Parquet writer
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                            HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(parquetFilePath), new Configuration()))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize(128 * 1024) // 128 KB
                    .build();


            CSVParser csvParserWriter = getCsvParser(csvFilePath, delimiter);

            List<String> headers = csvParserWriter.getHeaderNames();


            for (CSVRecord csvRecord : csvParserWriter) {
                GenericRecord record = new GenericData.Record(schema);
                System.out.println(csvRecord);
                for (String header : headers) {

                    Schema.Field field = schema.getField(header);
                    if (field != null) {
                        Schema nonNullableSchema = getNonNullableSchema(field.schema());
                        Schema.Type fieldType = nonNullableSchema.getType();

                        String rawValue = csvRecord.get(header);

                        Object value = parseValue(rawValue, fieldType);
                        System.out.println("  | "+ header+"  | "+value+"  | "+fieldType);
                        record.put(header, value);
                    } else {
                        System.out.println("Field not found in schema: " + header);
                    }
                }

                writer.write(record);
            }

            writer.close();
            System.out.println("Parquet file successfully created at: " + parquetFilePath);

            //ParquetFileAnalyzer parquetFileAnalyzer = new ParquetFileAnalyzer(parquetFilePath);
            //parquetFileAnalyzer.print();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Infer Avro schema from the CSV headers and a sample of its rows.
     */
    private static Schema inferSchema(CSVParser csvParser) throws Exception {
        List<String> headers = csvParser.getHeaderNames();

        Map<String, Schema.Type> inferredTypes = new HashMap<>();

        // Sample the first rows to infer types
        long sampleSize = 20;

        //if (csvParser.getRecords().size() < 100) sampleSize = csvParser.getRecords().size();
        //System.out.println("Detected headers: " + headers + " | CSV Records: " + csvParser.getRecords().size() + " | Samples: "+sampleSize);
        int count = 0;
        for (CSVRecord record : csvParser) {
            for (String header : headers) {

                String value = record.get(header);

                Schema.Type inferredType = inferType(value, inferredTypes.get(header));
                System.out.println(count +"  | "+ header+"  | "+value+"  | "+inferredType);
                inferredTypes.put(header, inferredType);
            }
            if (++count >= sampleSize) break;
        }

        // Dynamically build the Avro schema
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("DynamicRecord");
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        for (String header : headers) {
            Schema.Type type = inferredTypes.getOrDefault(header, Schema.Type.STRING);
            switch (type) {
                case INT -> fieldAssembler.name(header).type().nullable().intType().noDefault();
                case FLOAT -> fieldAssembler.name(header).type().nullable().floatType().noDefault();
                case BOOLEAN -> fieldAssembler.name(header).type().nullable().booleanType().noDefault();
                case STRING -> fieldAssembler.name(header).type().nullable().stringType().noDefault();
                default -> fieldAssembler.name(header).type().nullable().stringType().noDefault();
            }
        }

        return fieldAssembler.endRecord();
    }

    /**
     * Infer the data type based on the CSV value.
     */
    private static Schema.Type inferType(String value, Schema.Type currentType) {
        if (value == null || value.isEmpty()) {
            return currentType != null ? currentType : Schema.Type.STRING;
        }

        try {
            Integer.parseInt(value);
            return Schema.Type.INT;
        } catch (NumberFormatException ignored) {}

        try {
            Float.parseFloat(value.replace(",", "."));
            return Schema.Type.FLOAT;
        } catch (NumberFormatException ignored) {}

        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Schema.Type.BOOLEAN;
        }

        return Schema.Type.STRING;
    }

    /**
     * Convert the CSV value to the corresponding Avro schema type.
     */
    private static Object parseValue(String value, Schema.Type type) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        switch (type) {
            case STRING:
                return value;
            case INT:
                Integer element = null;
                try {
                    element = Integer.parseInt(value);
                } catch (NumberFormatException nfe) {
                    System.out.println(nfe);
                }
                return element;
            case FLOAT:
                return Float.parseFloat(value.replace(",", "."));
            case DOUBLE:
                return Double.parseDouble(value.replace(",", "."));
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /**
     * Extracts the non-nullable schema from a UNION type.
     */
    private static Schema getNonNullableSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema subSchema : schema.getTypes()) {
                if (subSchema.getType() != Schema.Type.NULL) {
                    return subSchema;
                }
            }
        }
        return schema; // Returns the schema itself if it is not UNION
    }

    private static CSVParser getCsvParser(String csvFilePath, char delimiter) throws IOException {
        FileReader reader = new FileReader(csvFilePath);
        CSVParser csvParser = new CSVParser(
                reader,
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(delimiter) // Set delimiter to semicolon
        );
        return csvParser;
    }
}
