package com.scicrop.infinitestack;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class Csv2ParquetTest {

    private static final String CSV_FILE_PATH = "/tmp/csv2parquet-test.csv";
    private static final String PARQUET_FILE_PATH = "/tmp/csv2parquet-test.parquet";
    private static final String CSV_DELIMITER = ",";

    @BeforeAll
    public static void setup() throws IOException {
        // Delete CSV file if it exists
        File csvFile = new File(CSV_FILE_PATH);
        if (csvFile.exists()) {
            csvFile.delete();
        }

        // Create the CSV file with test data
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE_PATH))) {
            writer.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n");
            for (int i = 1; i <= 20; i++) {
                writer.write(i + ",SC" + i + ",Sample Description " + i + "," + (i * 2) + ",2023-01-0" + (i % 10 + 1) + "," + (i * 1.5) + ",CUST" + i + ",Country" + i + "\n");
            }
        }

        // Delete Parquet file if it exists
        File parquetFile = new File(PARQUET_FILE_PATH);
        if (parquetFile.exists()) {
            parquetFile.delete();
        }
    }

    @Test
    public void testCsvToParquetConversion() {
        // Test CSV to Parquet conversion
        GenericCSVToParquet.main(new String[]{CSV_FILE_PATH, PARQUET_FILE_PATH, CSV_DELIMITER});

        // Check if Parquet file is created
        File parquetFile = new File(PARQUET_FILE_PATH);
        Assertions.assertTrue(parquetFile.exists(), "Parquet file should be created.");
        Assertions.assertTrue(parquetFile.length() > 0, "Parquet file should not be empty.");
    }

    @Test
    public void testParquetFileAnalysis() {
        // Analyze the Parquet file
        ParquetFileAnalyzer analyzer = new ParquetFileAnalyzer(PARQUET_FILE_PATH);

        // Validate total records
        long totalRecords = analyzer.countRecords();
        Assertions.assertEquals(20, totalRecords, "Parquet file should have 20 records.");

        // Validate column statistics
        Map<String, Map<String, Object>> stats = analyzer.generateColumnStatistics();
        Assertions.assertTrue(stats.containsKey("InvoiceNo"), "Column InvoiceNo should exist.");
        Assertions.assertEquals(20, stats.get("InvoiceNo").get("Unique Values"), "InvoiceNo should have 20 unique values.");
        Assertions.assertEquals(0L, stats.get("InvoiceNo").get("Null Values"), "InvoiceNo should have no null values.");
    }

    @Test
    public void testSchemaGeneration() {
        // Generate schema dynamically
        Map<String, Class<?>> fieldMap = Map.of(
                "InvoiceNo", Integer.class,
                "StockCode", String.class,
                "Description", String.class,
                "Quantity", Integer.class,
                "InvoiceDate", String.class,
                "UnitPrice", Double.class,
                "CustomerID", String.class,
                "Country", String.class
        );

        Schema schema = SchemaGenerator.getSchemaByMap(fieldMap);

        // Validate schema structure
        Assertions.assertNotNull(schema, "Schema should not be null.");
        Assertions.assertEquals("DynamicRecord", schema.getName(), "Schema name should be DynamicRecord.");
        Assertions.assertEquals(8, schema.getFields().size(), "Schema should have 8 fields.");
        Assertions.assertEquals("int", schema.getField("InvoiceNo").schema().getTypes().get(0).getType().getName(), "InvoiceNo field should be of type int.");
    }
}
