
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
            .master("local[1]")
            .appName("02-app-1743")
            .getOrCreate()
        )

# Parse the arguments
args = parse_args()
print(args)

# load df
df = spark.read.format('csv').load('./data/basic_csv/basic_file.csv')

# Stop the SparkSession
spark.stop()