from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Demo PySpark Job") \
    .getOrCreate()

# Read the CSV file
books = spark.read.csv("gs://{BUCKET}/books.csv", header=True, inferSchema=True)

# Show the initial data
print("Initial Data:")
books.show()

# Filter out the books with less than 200 pages
filtered_books = books.filter(col("number_of_pages") > 200)

# Show the filtered data
print("Filtered Data:")
filtered_books.show()

# Calculate the average number of pages
average_pages = filtered_books.agg(avg("number_of_pages")).first()[0]

# Print the result
print(f"Average number of pages in books with more than 200 pages: {average_pages}")

# Stop the SparkSession
spark.stop()
