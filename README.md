# Csv2Parquet

🚀 **Csv2Parquet** is a Java-based project that provides utilities to transform CSV files into optimized Parquet files, analyze Parquet data, and generate dynamic Avro schemas. This tool is perfect for engineers and data scientists working with large datasets who want to leverage the efficiency of the Parquet format.

## 🌟 Features

- **CSV to Parquet Conversion**: Seamlessly convert CSV files into compressed Parquet files.
- **Dynamic Schema Inference**: Automatically infer the schema from CSV headers and values.
- **Parquet File Analysis**: Inspect and extract statistics from Parquet files to ensure data quality.
- **Custom Avro Schema Generator**: Create Avro schemas dynamically from Java maps.

## 📂 Project Structure

This repository contains three primary classes:

1. **`GenericCSVToParquet`**: Converts CSV files to Parquet format.
2. **`ParquetFileAnalyzer`**: Analyzes Parquet files and provides insights such as record count and column statistics.
3. **`SchemaGenerator`**: Dynamically generates Avro schemas from a Java `Map` of field names and types.

## 🔧 Prerequisites

- Java 11 or higher
- Maven

## 📦 Dependencies

Ensure the following dependencies are included in your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro</artifactId>
        <version>1.11.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-avro</artifactId>
        <version>1.12.3</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-common</artifactId>
        <version>3.3.6</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>3.3.6</version>
    </dependency>
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-csv</artifactId>
        <version>1.9.0</version>
    </dependency>
</dependencies>
```

## 🚀 How to Use

### Running via Main Method

1. **`GenericCSVToParquet`**
   - Converts a CSV file into a Parquet file.
   - Modify the file paths in the `main` method and run the class.

   ```bash
   java com.scicrop.infinitestack.GenericCSVToParquet
   ```

2. **`ParquetFileAnalyzer`**
   - Analyze the generated Parquet file.
   - Update the file path in the `main` method and run:

   ```bash
   java com.scicrop.infinitestack.ParquetFileAnalyzer
   ```

3. **`SchemaGenerator`**
   - Generate Avro schemas dynamically from a `Map`.
   - Run the `main` method to see the generated schema:

   ```bash
   java com.scicrop.infinitestack.SchemaGenerator
   ```

### Integrating into Your Project

1. Add this repository to your project as a dependency or clone it locally.

2. Use the provided classes:

   ```java
   // Example: Convert CSV to Parquet
   GenericCSVToParquet converter = new GenericCSVToParquet();
   converter.convert("/path/to/input.csv", "/path/to/output.parquet");

   // Example: Analyze a Parquet file
   ParquetFileAnalyzer analyzer = new ParquetFileAnalyzer("/path/to/output.parquet");
   analyzer.print();

   // Example: Generate Avro Schema
   Map<String, Class<?>> fieldMap = Map.of(
       "InvoiceNo", String.class,
       "Quantity", Integer.class
   );
   Schema schema = SchemaGenerator.getSchemaByMap(fieldMap);
   System.out.println(schema);
   ```

## 🛠️ Examples

### CSV to Parquet Conversion

```java
String csvFilePath = "/tmp/sales.csv";
String parquetFilePath = "/tmp/sales.parquet";

GenericCSVToParquet.main(new String[]{csvFilePath, parquetFilePath});
```

### Parquet Analysis

```java
ParquetFileAnalyzer analyzer = new ParquetFileAnalyzer("/tmp/sales.parquet");
analyzer.print();
```

### Avro Schema Generation

```java
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
Schema schema = SchemaGenerator.getSchemaByMap(fieldMap);
System.out.println(schema.toString(true));
```

## 📝 License

This project is licensed under the [Apache License 2.0](LICENSE).

## ❤️ Contributing

Contributions are welcome! Feel free to submit issues and pull requests to improve the project.

## 🌟 Acknowledgements

- **Apache Avro** for schema generation.
- **Apache Parquet** for efficient storage.
- **Apache Commons CSV** for CSV parsing.

---

Enjoy using **Csv2Parquet**! 🌟 Feel free to reach out if you have any questions or feedback.

