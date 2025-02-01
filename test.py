
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
            .getOrCreate()
        )

configurations = spark.sparkContext.getConf().getAll()
print("#################################")
for item in configurations: print(item)
print("#################################")

# Parse the arguments
args = parse_args()
print(args)

# load df
df = spark.read.format('csv').load('./data/500mb_dataset/dataset.csv')

df.write.mode('overwrite').save('./data/to_save/500mb_dataset/dataset')

# Stop the SparkSession
spark.stop()