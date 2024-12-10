# Good and Bad Practices in PySpark and Spark SQL

## PySpark

### **Bad Practices**

1.  **Not using DataFrame API's optimized operations** (e.g., using `map()` instead of built-in DataFrame operations)
    
    -   **Example (Bad):**
        
        ```python
        rdd = sc.parallelize([('Alice', 1), ('Bob', 2), ('Charlie', 3)])
        rdd_map = rdd.map(lambda x: (x[0], x[1] * 2))
        result = rdd_map.collect()
        ```
        
    -   **Issue:** Using `map()` results in a lower-level RDD transformation, which does not take advantage of Spark's optimizations for DataFrame APIs.
2.  **Not using `cache()` or `persist()` when reusing DataFrames**
    
    -   **Example (Bad):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        result1 = df.filter(df['column1'] > 100).count()
        result2 = df.filter(df['column2'] < 50).count()
        ```
        
    -   **Issue:** Data is read and computed twice without caching, leading to unnecessary recomputations.
3.  **Performing operations on unpartitioned DataFrames**
    
    -   **Example (Bad):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        result = df.filter(df['column1'] > 100).groupBy('column2').agg({'column3': 'sum'}).collect()
        ```
        
    -   **Issue:** Spark may need to shuffle data across nodes because the DataFrame isn't partitioned based on `column2`, which causes a performance bottleneck.
4.  **Using `.show()` on large DataFrames**
    
    -   **Example (Bad):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        df.show()  # Displaying large dataset in the console
        ```
        
    -   **Issue:** This is inefficient when working with large datasets, as it can overwhelm the driver and lead to poor performance.

----------

### **Good Practices**

1.  **Use DataFrame operations over RDDs**
    
    -   **Example (Good):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        result = df.filter(df['column1'] > 100).groupBy('column2').agg({'column3': 'sum'})
        ```
        
    -   **Reason:** DataFrame API is optimized for Spark's Catalyst optimizer and provides better performance over RDD-based operations.
2.  **Caching when using the same DataFrame multiple times**
    
    -   **Example (Good):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        df.cache()
        result1 = df.filter(df['column1'] > 100).count()
        result2 = df.filter(df['column2'] < 50).count()
        ```
        
    -   **Reason:** Caching avoids recomputation by storing the DataFrame in memory, improving performance for repeated operations.
3.  **Repartitioning DataFrames for better parallelism**
    
    -   **Example (Good):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        df_repartitioned = df.repartition(200, 'column2')
        result = df_repartitioned.filter(df['column1'] > 100).groupBy('column2').agg({'column3': 'sum'})
        ```
        
    -   **Reason:** Repartitioning based on the column used for aggregation reduces shuffling and improves performance.
4.  **Using `.select()` instead of `.collect()` for large datasets**
    
    -   **Example (Good):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        result = df.select("column1", "column2").filter(df['column1'] > 100)
        result.show()  # For showing only a small subset of the data
        ```
        
    -   **Reason:** Use `.select()` to minimize the data being processed or displayed, and avoid collecting large datasets to the driver node.

----------

## Spark SQL

### **Bad Practices**

1.  **Using `SELECT *` in queries with large tables**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT * FROM large_table;
        ```
        
    -   **Issue:** Selecting all columns results in reading the entire table, even if only a few columns are necessary, leading to high I/O.
2.  **Performing multiple joins without optimizing the query**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT A.id, B.name, C.date, D.amount
        FROM tableA A
        JOIN tableB B ON A.id = B.id
        JOIN tableC C ON A.id = C.id
        JOIN tableD D ON A.id = D.id
        WHERE A.status = 'active' AND B.value > 100;
        ```
        
    -   **Issue:** This query performs multiple joins without any optimization. It can cause unnecessary shuffling and slow performance when the tables are large.
3.  **Using subqueries in `SELECT` statements**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT id, (SELECT AVG(value) FROM tableB WHERE tableB.id = tableA.id) AS avg_value
        FROM tableA;
        ```
        
    -   **Issue:** Subqueries in `SELECT` statements are inefficient because they are executed for each row, leading to redundant computations.
4.  **Using complex aggregations without partitioning**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT id, SUM(value), COUNT(*) FROM large_table GROUP BY id;
        ```
        
    -   **Issue:** A large aggregation without partitioning leads to unnecessary shuffling of data across nodes, which is inefficient.

----------

### **Good Practices**

1.  **Use explicit column names instead of `SELECT *`**
    
    -   **Example (Good):**
        
        ```sql
        SELECT id, name, date FROM large_table WHERE date > '2023-01-01';
        ```
        
    -   **Reason:** This query reads only the necessary columns, reducing I/O and improving performance.
2.  **Optimize joins by using `broadcast` for small tables**
    
    -   **Example (Good):**
        
        ```sql
        SELECT A.id, B.name, C.date
        FROM tableA A
        JOIN broadcast(tableB) B ON A.id = B.id
        JOIN tableC C ON A.id = C.id;
        ```
        
    -   **Reason:** Broadcasting small tables (`tableB`) avoids shuffling and improves join performance.
3.  **Avoid subqueries in `SELECT` and use `JOIN` instead**
    
    -   **Example (Good):**
        
        ```sql
        SELECT A.id, AVG(B.value) AS avg_value
        FROM tableA A
        JOIN tableB B ON A.id = B.id
        GROUP BY A.id;
        ```
        
    -   **Reason:** Avoiding subqueries improves performance by reducing redundant computations. Joins can be optimized better by Spark's Catalyst optimizer.
4.  **Repartition data before performing heavy aggregations**
    
    -   **Example (Good):**
        
        ```sql
        SET spark.sql.shuffle.partitions = 200;  -- Adjust based on data size
        SELECT id, SUM(value), COUNT(*) FROM large_table
        DISTRIBUTE BY id
        GROUP BY id;
        ```
        
    -   **Reason:** Repartitioning and using `DISTRIBUTE BY` ensures better parallelism and minimizes shuffling during aggregation.
5.  **Cache intermediate results for repeated use**
    
    -   **Example (Good):**
        
        ```sql
        CACHE TABLE tableA;
        SELECT id, COUNT(*) FROM tableA WHERE status = 'active' GROUP BY id;
        ```
        
    -   **Reason:** Caching intermediate tables avoids recomputing the same data and speeds up subsequent queries.

----------

## Conclusion

By following **good practices** in both **PySpark** and **Spark SQL**, you can take full advantage of Spark's optimizations, ensuring faster processing and reducing unnecessary resource consumption. Make sure to focus on:

-   Avoiding inefficient operations (e.g., `SELECT *`, unnecessary joins, subqueries).
-   Leveraging **DataFrame API** and **broadcast joins** for better performance.
-   **Caching** and **partitioning** data to optimize query execution.

By following these best practices, your Spark jobs will run more efficiently and scale better as your data grows.



Here are some additional good and bad practices that will help you improve your Spark jobs.

----------

## Additional Good and Bad Practices in PySpark and Spark SQL

### PySpark

#### **Bad Practices**

5.  **Using non-partitioned `DataFrame` for joins on large datasets**
    
    -   **Example (Bad):**
        
        ```python
        df1 = spark.read.csv("large_data1.csv")
        df2 = spark.read.csv("large_data2.csv")
        result = df1.join(df2, "key").select("key", "value").show()
        
        ```
        
    -   **Issue:** If `df1` and `df2` are not partitioned appropriately, the join operation will trigger expensive shuffling across nodes, which slows down the job.
6.  **Using `.collect()` on large datasets**
    
    -   **Example (Bad):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        result = df.filter(df['column1'] > 100).collect()  # Collecting all data to driver
        
        ```
        
    -   **Issue:** Calling `.collect()` brings all data to the driver, which can cause memory overflow and slowdowns if the dataset is large.
7.  **Not handling skewed data during groupBy operations**
    
    -   **Example (Bad):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        result = df.groupBy("column1").agg({"column2": "sum"})
        
        ```
        
    -   **Issue:** If `column1` has highly skewed data, it may cause an uneven distribution of data across partitions, leading to bottlenecks.
8.  **Using `for` loops in PySpark with large DataFrames**
    
    -   **Example (Bad):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        for row in df.collect():  # Iterating over all rows
            process(row)
        
        ```
        
    -   **Issue:** Iterating through rows with `.collect()` is inefficient and goes against the parallel nature of Spark. You should leverage Spark's DataFrame API instead.

----------

#### **Good Practices**

5.  **Partition data before performing joins**
    
    -   **Example (Good):**
        
        ```python
        df1 = spark.read.csv("large_data1.csv")
        df2 = spark.read.csv("large_data2.csv")
        df1_partitioned = df1.repartition(200, "key")
        df2_partitioned = df2.repartition(200, "key")
        result = df1_partitioned.join(df2_partitioned, "key").select("key", "value")
        
        ```
        
    -   **Reason:** Partitioning both datasets on the join key ensures that the join is done locally within each partition, reducing the need for shuffling.
6.  **Avoid using `.collect()` on large datasets; use `.show()` or `.take()` for small results**
    
    -   **Example (Good):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        df.filter(df['column1'] > 100).show(5)  # Show top 5 rows for debugging
        
        ```
        
    -   **Reason:** Instead of collecting all data, use `.show()` for debugging and `.take()` for retrieving a sample of data, which avoids overwhelming the driver node.
7.  **Handle skewed data with techniques like salting**
    
    -   **Example (Good):**
        
        ```python
        from pyspark.sql.functions import col, rand
        
        df = spark.read.csv("large_data.csv")
        df_with_salt = df.withColumn("salt", (rand() * 10).cast("int"))
        result = df_with_salt.groupBy("salt", "column1").agg({"column2": "sum"})
        
        ```
        
    -   **Reason:** Salting splits skewed groups into smaller chunks, reducing data imbalance during aggregations or joins.
8.  **Leverage the power of Spark’s built-in DataFrame operations**
    
    -   **Example (Good):**
        
        ```python
        df = spark.read.csv("large_data.csv")
        df_filtered = df.filter(df['column1'] > 100)
        df_grouped = df_filtered.groupBy("column2").agg({"column3": "avg"})
        df_grouped.show()
        
        ```
        
    -   **Reason:** Avoid low-level iteration like `for` loops. Use DataFrame transformations that Spark can optimize through the Catalyst optimizer.

----------

### Spark SQL

#### **Bad Practices**

5.  **Using a lot of `ORDER BY` clauses on large datasets**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT id, value FROM large_table ORDER BY value;
        
        ```
        
    -   **Issue:** `ORDER BY` operations require sorting the entire dataset, which can be expensive, especially on large datasets. This can cause excessive disk I/O and slow performance.
6.  **Running queries with non-selective filters**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT * FROM large_table WHERE column1 > 10;
        
        ```
        
    -   **Issue:** Filters that do not significantly reduce the dataset size lead to inefficient query plans, as Spark may still scan a large portion of the data.
7.  **Using inefficient window functions without partitioning**
    
    -   **Example (Bad):**
        
        ```sql
        SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) AS rank
        FROM large_table;
        
        ```
        
    -   **Issue:** Window functions that do not leverage partitioning can result in large shuffles, degrading performance.
8.  **Running `UPDATE` and `DELETE` queries on large datasets**
    
    -   **Example (Bad):**
        
        ```sql
        DELETE FROM large_table WHERE value < 100;
        
        ```
        
    -   **Issue:** Spark is optimized for append-only workloads. Running delete operations on large datasets can be inefficient and should be avoided if possible. This can lead to the creation of additional files and complicates data management.

----------

#### **Good Practices**

5.  **Avoid using `ORDER BY` on large datasets; use partitioning or limit results**
    
    -   **Example (Good):**
        
        ```sql
        SELECT id, value FROM large_table WHERE column1 > 100 LIMIT 100;
        
        ```
        
    -   **Reason:** Limiting the result size helps avoid sorting large datasets. If sorting is necessary, try partitioning your data first.
6.  **Apply selective filters early to reduce data size**
    
    -   **Example (Good):**
        
        ```sql
        SELECT id, value FROM large_table WHERE column1 = 'active' AND column2 > 50;
        
        ```
        
    -   **Reason:** Applying filters early reduces the amount of data that needs to be processed, speeding up queries.
7.  **Use window functions with proper partitioning to reduce shuffling**
    
    -   **Example (Good):**
        
        ```sql
        SELECT id, value, ROW_NUMBER() OVER (PARTITION BY id ORDER BY value) AS rank
        FROM large_table;
        
        ```
        
    -   **Reason:** Partitioning window functions ensures that the computation happens locally on each partition, minimizing the need for shuffling and improving performance.
8.  **Avoid running `UPDATE` or `DELETE` operations on large tables, prefer append operations**
    
    -   **Example (Good):** Instead of running:
        
        ```sql
        DELETE FROM large_table WHERE value < 100;
        
        ```
        
        -   **Approach (Good):** Instead of deleting, mark records or create a new table with the updated data:
        
        ```sql
        CREATE OR REPLACE TABLE updated_table AS
        SELECT * FROM large_table WHERE value >= 100;
        
        ```
        
    -   **Reason:** Avoiding updates and deletes helps maintain the performance benefits of Spark, which is designed for append-only operations. It also prevents the overhead of managing large amounts of deleted data.

----------

## Conclusion

By understanding both **good** and **bad** practices in **PySpark** and **Spark SQL**, you can improve the performance and scalability of your Spark jobs. Here are some additional tips:

-   Always **partition** and **repartition** data intelligently based on your queries to reduce shuffle.
-   Avoid **collecting** large datasets to the driver, instead use `.show()` or `.take()` for debugging.
-   Prefer **broadcast joins** for small lookup tables, and use **window functions** with partitioning for efficiency.
-   **Cache** intermediate results if reused multiple times.
-   Optimize **SQL queries** by using selective filters early and avoiding expensive operations like `ORDER BY` on large datasets.

By following these practices, you will be able to write more efficient, faster, and scalable Spark code.
