from graphframes import GraphFrame
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import desc, col, count
from pyspark.sql.functions import explode, first
from pyspark.sql.functions import concat_ws
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from urllib.parse import urlparse


spark = SparkSession.builder.appName("MyApp").getOrCreate()

schema = StructType([
    StructField("parent", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("html_text", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("category", ArrayType(StringType()), True),
    StructField("url", StringType(), True),
])


def truncate_label(label, max_length=14):
    if len(label) > max_length:
        return label[:max_length] + "..."
    else:
        return label

def extract_domain(url):
    parsed_url = urlparse(url)
    domain = '{uri.netloc}'.format(uri=parsed_url)
    return domain


extract_domain_udf = udf(extract_domain, StringType())



df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").format("json").schema(schema).load("data.json")
data_with_domain = df.withColumn("domain", extract_domain_udf("url"))

vertices = df.selectExpr("url as id").distinct()

edges = df.selectExpr("url as src", "parent as dst")
graph = GraphFrame(vertices, edges)
#result = graph.pageRank(resetProbability=0.15, tol=0.01)
InnerDegree = graph.inDegrees
InnerDegree = InnerDegree.sort(desc("inDegree")).limit(10)
InnerDegree.show()

spark.sparkContext.setCheckpointDir("/home/bigdata/Desktop")
connected_components = graph.connectedComponents()
#scc = graph.stronglyConnectedComponents(maxIter=10)

import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, count, desc

# Assuming you have computed the connected_components DataFrame already
top_10_components = connected_components.groupBy("component").count().sort(col("count").desc()).limit(10)
component_id = top_10_components.select("component").tail(1)[0][0]
# Iterate through each of the top 10 components
for row in top_10_components.collect():
	component_id = row["component"]
	print(f"Processing component {component_id}")
	component_dir = f"component_{component_id}"
	os.makedirs(component_dir, exist_ok=True)
    # Filter the connected_components DataFrame to keep only the current component
	current_component = connected_components.filter(col("component") == component_id)
    # Join the current_component DataFrame with the original graph.vertices DataFrame on the 'id' column
	current_vertices = current_component.join(vertices, "id", "inner").select(vertices.columns)
    # Filter the edges of the original graph for the current connected component
	current_edges = graph.edges.join(current_vertices, graph.edges["src"] == current_vertices["id"], "inner").select(graph.edges.columns)
    	# Create a new GraphFrame using the resulting vertices and edges of the current connected component
	current_graph = GraphFrame(current_vertices, current_edges)
	# Run PageRank on the current GraphFrame
	pagerank_results = current_graph.pageRank(resetProbability=0.15, tol=0.01)
	pagerank_results_10 = pagerank_results.vertices.select("id", "pagerank").orderBy(desc("pagerank")).limit(10)
	pagerank_results_10.write.csv(os.path.join(component_dir, "pagerank_results.csv"), header=True)
	
	current_vertices_with_keywords = current_vertices.join(data_with_domain.select("domain","url", "keywords"),current_vertices["id"] == df["url"]).select("domain","id", "keywords")
	domains_count = current_vertices_with_keywords.groupBy("domain").agg(count("*").alias("domain_count"))
	top_10_domains = domains_count.sort(desc("domain_count")).limit(10)
	exploded_keywords = current_vertices_with_keywords.select("domain","id", explode("keywords").alias("keyword"))
	top_10_keywords = exploded_keywords.groupBy("keyword").count().sort(desc("count")).limit(10)
	
	keywords_vs_domains = exploded_keywords.groupBy("domain", "keyword").agg(count("*").alias("num_occurrences")).filter(col("domain").isin([row["domain"] for row in top_10_domains.collect()])).filter(col("keyword").isin(([row["keyword"] for row in top_10_keywords.collect()])))
	from pyspark.ml.feature import StringIndexer
	string_indexer = StringIndexer(inputCol="domain", outputCol="domain_index")
	model = string_indexer.fit(keywords_vs_domains)
	keywords_vs_domains = model.transform(keywords_vs_domains)
	keywords_vs_domains_pd = keywords_vs_domains.toPandas()
	heatmap_data = keywords_vs_domains_pd.pivot(index="domain_index", columns="keyword", values="num_occurrences")
	sns.set()
	plt.figure(figsize=(10, 12))  # Adjust the figure size
	ax = sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu", fmt="g")
	ax.set_title(f"Heatmap for Component {component_id}")
	plt.savefig(os.path.join(component_dir, "heatmap.eps"), format="eps", dpi=500)
	in_degrees = current_graph.inDegrees
	top_10_vertices_indegree = in_degrees.sort(desc("inDegree")).limit(10)
	top_10_vertices_with_keywords = top_10_vertices_indegree.join(df.select("url", "keywords"),
	top_10_vertices_indegree["id"] == df["url"]).select("id", "inDegree", "keywords")
	top_10_vertices_with_keywords = top_10_vertices_with_keywords.withColumn('keywords', concat_ws('|', 'keywords'))
	top_10_vertices_with_keywords.write.csv(os.path.join(component_dir, "top_10_indegree_vertices.csv"), header=True)

""""
Here, I have used the Pagerank which is  a metric to measure the importance of nodes in a graph. It assigns a score 
to each node in a graph based on its importance in the network, with the assumption that a node is more important if
it has more incoming links from other important nodes. The algorithm computes the score iteratively by calculating the
probability that a random walk on the graph will arrive at each node, with a higher score indicating a higher likelihood
of being visited
"""
# ~/spark/bin/pyspark --packages graphframes:graphframes:0.8.0-spark3.0-s_2.12
# stop the SparkSession
spark.stop()

