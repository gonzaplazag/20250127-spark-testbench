from pyspark.sql import SparkSession
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str)
    parser.add_argument("--age", type=int)

    args = parser.parse_args()

    arguments = {
        "name": args.name if args.name is not None else "sin-nombre",
        "age": args.age if args.age is not None else -1
    }

    return arguments

if __name__=="__main__":

    spark = (
        SparkSession
        .builder
        .appName("ExampleApp")
        .getOrCreate()
    )

# Parse the arguments
args = parse_args()

print(args['name'])
print(args['age'])
print("#####################################")

# Create a DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# save DataFrame
df.write.mode('overwrite').save("./data/test_table")

# Stop the SparkSession
spark.stop()