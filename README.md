# Csv2Parquet

🚀 **Csv2Parquet** is a Java-based library designed to simplify the conversion of CSV files into Parquet format, dynamically generate Avro schemas, and perform comprehensive Parquet file analysis. This tool is optimized for performance and scalability, making it ideal for processing large datasets.

## 🌟 Features

- **CSV to Parquet Conversion**: Convert CSV files to optimized and compressed Parquet files effortlessly.
- **Dynamic Schema Inference**: Automatically infer Avro schemas from CSV headers and sample data.
- **Parquet File Analysis**: Analyze Parquet files, including record counts and column statistics.
- **Schema Generator**: Create Avro schemas programmatically from Java `Map` objects.
- **JUnit 5 Test Suite**: Comprehensive unit tests to validate the library's functionality.

## 📂 Project Structure

- **`GenericCSVToParquet`**: Handles CSV to Parquet conversion with dynamic schema inference.
- **`ParquetFileAnalyzer`**: Provides tools to analyze Parquet files, including record inspection and statistics generation.
- **`SchemaGenerator`**: Dynamically generates Avro schemas based on field definitions.
- **`Csv2ParquetTest`**: JUnit 5 test class to validate the main functionalities of the library.

## 🔧 Prerequisites

- Java 17 or higher
- Maven 3.6+

## 📦 Dependencies

The following dependencies are required and included in the `pom.xml`:

```xml
<dependencies>
    <!-- Apache Parquet -->
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-avro</artifactId>
        <version>1.12.3</version>
    </dependency>

    <!-- Apache Commons CSV -->
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-csv</artifactId>
        <version>1.10.0</version>
    </dependency>

    <!-- Apache Avro -->
    <dependency>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro</artifactId>
        <version>1.11.2</version>
    </dependency>

    <!-- Apache Hadoop -->
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

    <!-- JUnit 5 -->
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>5.10.0</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## 🚀 How to Use

### Running via Main Method

1. **Convert CSV to Parquet**
   - Use `GenericCSVToParquet` to process a CSV file:

     ```bash
     java com.scicrop.infinitestack.GenericCSVToParquet /path/to/input.csv /path/to/output.parquet ,
     ```
     Replace `,` with your CSV delimiter if different.

2. **Analyze Parquet Files**
   - Use `ParquetFileAnalyzer` to inspect and analyze Parquet files:

     ```bash
     java com.scicrop.infinitestack.ParquetFileAnalyzer /path/to/output.parquet
     ```

3. **Generate Avro Schema**
   - Use `SchemaGenerator` to create Avro schemas dynamically:

     ```bash
     java com.scicrop.infinitestack.SchemaGenerator
     ```

### Integrating into Your Project

1. Add this repository as a dependency or clone it locally.
2. Use the classes programmatically in your application:

   ```java
   // Convert CSV to Parquet
   GenericCSVToParquet.process("/tmp/input.csv", "/tmp/output.parquet", ',');

   // Analyze a Parquet file
   ParquetFileAnalyzer analyzer = new ParquetFileAnalyzer("/tmp/output.parquet");
   analyzer.print();

   // Generate an Avro schema
   Map<String, Class<?>> fieldMap = Map.of(
       "InvoiceNo", String.class,
       "Quantity", Integer.class
   );
   Schema schema = SchemaGenerator.getSchemaByMap(fieldMap);
   System.out.println(schema.toString(true));
   ```

## 🧪 Running Tests

1. Navigate to the project directory.
2. Run the tests with Maven:

   ```bash
   mvn test
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
