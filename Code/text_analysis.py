from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,ArrayType
from pyspark.sql.functions import desc,explode,count,arrays_zip,col

# create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType

schema = StructType([
    StructField("parent", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("html_text", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("category", ArrayType(StringType()), True),
    StructField("url", StringType(), True),
])
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from urllib.parse import urlparse

# Define a UDF to extract the domain from a URL
def extract_domain(url):
    parsed_url = urlparse(url)
    domain = '{uri.netloc}'.format(uri=parsed_url)
    return domain


def truncate_label(label, max_length=5):
    if len(label) > max_length:
        return label[:max_length] + "..."
    return label


# Register the UDF
extract_domain_udf = udf(extract_domain, StringType())


data = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").format("json").schema(schema).load("data.json")
data  = data.select("url","parent","keywords","category")
data_with_domain = data.withColumn("domain", extract_domain_udf("url"))
#df_with_domain = df.withColumn("domain", expr("parse_url(url, 'HOST')"))
distinct_domains = data_with_domain.select("domain").distinct().count()
print(distinct_domains)


#count and show the top 10 domains 
domain_counts = data_with_domain.groupBy("domain").agg(count("*").alias("pages_count"))
domain_counts = domain_counts.sort(desc("pages_count"))
top_domains = domain_counts.filter(col("pages_count") > 200)
#top_10_domains.show()


#Select top keywords
keyword_counts = data.select(explode("keywords").alias("keyword"))\
                  .groupBy("keyword").count()
sorted_keyword_counts = keyword_counts.sort(desc("count"))
top_keywords = sorted_keyword_counts.limit(10)
#top_10_keywords.show()

#Visualization
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
keywords_vs_domains = data_with_domain.select("domain", explode(col("keywords")).alias("keyword")) \
    .groupBy("domain", "keyword") \
    .agg(count("*").alias("num_occurrences")) \
    .filter(col("domain").isin([row["domain"] for row in top_domains.collect()])) \
    .filter(col("keyword").isin([row["keyword"] for row in top_keywords.collect()]))

keywords_vs_domains = keywords_vs_domains.sort(desc("num_occurrences"))

top_keywords_list = top_keywords.select("keyword").rdd.flatMap(lambda x: x).collect()
top_domains_list = top_domains.select("domain").rdd.flatMap(lambda x: x).collect()
print(top_keywords_list)
print(top_domains_list)
keywords_vs_domains_pd = keywords_vs_domains.toPandas()
heatmap_data = keywords_vs_domains_pd.pivot(index="domain", columns="keyword", values="num_occurrences")

# Create the heatmap
sns.set()
plt.figure(figsize=(10, 12))  # Adjust the figure size
ax = sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu", fmt="g")

# Get the current y-axis labels
current_yticklabels = ax.get_yticklabels()

# Truncate y-axis labels and set them
new_yticklabels = [truncate_label(label.get_text()) for label in current_yticklabels]
ax.set_yticklabels(new_yticklabels)

# Rotate the labels and adjust the font size
plt.xticks(rotation=45, fontsize=8)
plt.yticks(rotation=0, fontsize=8)

# Save the heatmap to a postscript file
plt.savefig("heatmap.eps", format="eps", dpi=500)

# stop the SparkSession
spark.stop()



