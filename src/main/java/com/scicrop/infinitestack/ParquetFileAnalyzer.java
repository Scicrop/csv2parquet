package com.scicrop.infinitestack;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.util.HashMap;
import java.util.Map;

public class ParquetFileAnalyzer {

    private final String parquetFilePath;

    public ParquetFileAnalyzer(String parquetFilePath) {
        this.parquetFilePath = parquetFilePath;
    }

    /**
     * Counts the total number of records in the Parquet file.
     */
    public long countRecords() {
        long count = 0;

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(parquetFilePath), new Configuration()))
                .build()) {

            while (reader.read() != null) {
                count++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return count;
    }

    /**
     * Prints the first N records of the Parquet file.
     */
    public void printSampleRecords(int sampleSize) {
        System.out.println("Displaying the first " + sampleSize + " records from the Parquet file:");
        int count = 0;

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(parquetFilePath), new Configuration()))
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null && count < sampleSize) {
                System.out.println(record);
                count++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates basic statistics, such as count and unique values for each column.
     */
    public Map<String, Map<String, Object>> generateColumnStatistics() {
        Map<String, Map<String, Object>> columnStats = new HashMap<>();
        Map<String, Integer> uniqueValues = new HashMap<>();
        Map<String, Long> nullCounts = new HashMap<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(parquetFilePath), new Configuration()))
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                for (Schema.Field field : record.getSchema().getFields()) {
                    String fieldName = field.name();
                    Object value = record.get(fieldName);

                    // Count unique values
                    if (value != null) {
                        uniqueValues.put(fieldName, uniqueValues.getOrDefault(fieldName, 0) + 1);
                    } else {
                        nullCounts.put(fieldName, nullCounts.getOrDefault(fieldName, 0L) + 1);
                    }
                }
            }

            // Generate statistics
            for (String fieldName : uniqueValues.keySet()) {
                Map<String, Object> stats = new HashMap<>();
                stats.put("Unique Values", uniqueValues.getOrDefault(fieldName, 0));
                stats.put("Null Values", nullCounts.getOrDefault(fieldName, 0L));
                columnStats.put(fieldName, stats);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return columnStats;
    }

    /**
     * Main method to test the analysis.
     */
    public static void main(String[] args) {
        String parquetFilePath = "/tmp/sales.parquet";
        ParquetFileAnalyzer analyzer = new ParquetFileAnalyzer(parquetFilePath);
        analyzer.print();
    }

    /**
     * Executes the analysis and prints results.
     */
    public void print() {

        long totalRecords = countRecords();
        System.out.println("Total records in the file: " + totalRecords);

        // Display the first 5 records
        printSampleRecords(5);

        // Generate column statistics
        Map<String, Map<String, Object>> stats = generateColumnStatistics();
        System.out.println("Column Statistics:");
        stats.forEach((column, statMap) -> {
            System.out.println("Column: " + column);
            statMap.forEach((stat, value) -> System.out.println("  " + stat + ": " + value));
        });
    }
}
