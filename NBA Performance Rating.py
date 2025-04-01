# Databricks notebook source
# MAGIC %md
# MAGIC # NBA Best Performance using Real-time Data Processing with Azure Databricks (and Event Hubs)
# MAGIC
# MAGIC This notebook demonstrates a real-time data processing workflow in Databricks using Structured Streaming to ingest data from Event Hubs. Designed a Bronze-Silver-Gold architecture to refine and transform data to find the best performance from a basketball game, with an automated email alert to notify stakeholders of the top results.
# MAGIC
# MAGIC - Data Sources: Streaming data from IoT devices or social media feeds. (Simulated in Event Hubs)
# MAGIC - Ingestion: Azure Event Hubs for capturing real-time data.
# MAGIC - Processing: Azure Databricks for stream processing using Structured Streaming.
# MAGIC - Storage: Processed data stored Azure Data Lake (Delta Format).
# MAGIC
# MAGIC ### Azure Services Required
# MAGIC - Databricks Workspace
# MAGIC - Azure Data Lake Storage
# MAGIC - Azure Event Hub
# MAGIC
# MAGIC ### Azure Databricks Configuration Required
# MAGIC - Single Node Compute Cluster: `12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)`
# MAGIC - Maven Library installed on Compute Cluster: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data:
# MAGIC ### {
# MAGIC ###     "gameId": "G78901",
# MAGIC ###     "date": "2025-03-18",
# MAGIC ###     "team_stats": {
# MAGIC ###         "team": "Lakers",
# MAGIC ###         "total_points": 120,
# MAGIC ###         "total_assists": 30,
# MAGIC ###         "total_rebounds": 55,
# MAGIC ###         "total_steals": 8,
# MAGIC ###         "total_blocks": 7,
# MAGIC ###         "total_turnovers": 9,
# MAGIC ###         "total_fouls": 17
# MAGIC ###     },
# MAGIC ###     "players": [
# MAGIC ###         {"player": "LeBron James", "points": 32, "assists": 8, "rebounds": 10, "steals": 2, "blocks": 2, "fouls": 2, "turnovers": 2},
# MAGIC ###         {"player": "Anthony Davis", "points": 25, "assists": 4, "rebounds": 12, "steals": 1, "blocks": 4, "fouls": 3, "turnovers": 1},
# MAGIC ###         {"player": "D'Angelo Russell", "points": 18, "assists": 9, "rebounds": 4, "steals": 2, "blocks": 1, "fouls": 2, "turnovers": 2},
# MAGIC ###         {"player": "Austin Reaves", "points": 16, "assists": 6, "rebounds": 5, "steals": 3, "blocks": 0, "fouls": 3, "turnovers": 2}
# MAGIC ###     ],
# MAGIC ###     "quarter": 4,
# MAGIC ###     "time": "00:00"
# MAGIC ### }
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC The code block below creates the catalog and schemas for our solution. 
# MAGIC
# MAGIC The approach utilises a multi-hop data storage architecture (medallion), consisting of bronze, silver, and gold schemas

# COMMAND ----------

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze;")
except:
    print('check if bronze schema already exists')

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.silver;")
except:
    print('check if silver schema already exists')

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metadata.gold;")
except:
    print('check if gold schema already exists')

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Importing the libraries.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import smtplib
from email.mime.text import MIMEText


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC * Set up Azure Event hubs connection string.
# MAGIC * Defining JSON Schema
# MAGIC * Reading and writing the stream to Bronze Layer

# COMMAND ----------

# Event Hubs Configuration
connectionString = "Endpoint=sb://jack-namespace-demo.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=##############################################;EntityPath=eh_nba"

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
}

# COMMAND ----------

#JSON Schema
# No JSON parsing in Bronze layer, store raw data
player_schema = ArrayType(StructType([
    StructField("player", StringType()),
    StructField("points", IntegerType()),
    StructField("assists", IntegerType()),
    StructField("rebounds", IntegerType()),
    StructField("steals", IntegerType()),
    StructField("blocks", IntegerType()),
    StructField("fouls", IntegerType()),
    StructField("turnovers", IntegerType())
]))

team_stats_schema = StructType([
    StructField("team", StringType()),
    StructField("total_points", IntegerType()),
    StructField("total_assists", IntegerType()),
    StructField("total_rebounds", IntegerType()),
    StructField("total_steals", IntegerType()),
    StructField("total_blocks", IntegerType()),
    StructField("total_turnovers", IntegerType()),
    StructField("total_fouls", IntegerType())
])

json_schema = StructType([
    StructField("gameId", StringType()),
    StructField("date", StringType()),
    StructField("team_stats", team_stats_schema),
    StructField("players", player_schema),
    StructField("quarter", IntegerType()),
    StructField("time", StringType())
])

# COMMAND ----------

# Read raw streaming data from Event Hubs

df_bronze = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Write to Bronze Table
df_bronze.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/bronze/nba") \
    .outputMode("append") \
    .format("delta") \
    .toTable("hive_metastore.bronze.nba")

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Checking whether data is stored in Bronze NBA Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from hive_metastore.bronze.nba
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC * Reading the stream from the bronze 
# MAGIC * eventID is added for dropping duplicates
# MAGIC * Data is flattened into player and team stats
# MAGIC * Writing the transformed stream into silver layer

# COMMAND ----------

# Read raw streaming data from Bronze Table
df_silver = spark.readStream \
    .format("delta") \
    .table("hive_metastore.bronze.nba") \
    .withColumn("body", col("body").cast("string")) \
    .withColumn("body", from_json(col("body"), json_schema)) \
    .withColumn("eventId", expr("uuid()")) \
    .select("eventId", "body.*", col("enqueuedTime").alias("timestamp")) \
    .dropDuplicates(["eventId"])

# Flatten data in Silver Layer
df_silver_flat = df_silver.withColumn("player", explode(col("players"))) \
    .select("eventId", "gameId", "date", "team_stats.team", "team_stats.total_points", "team_stats.total_assists", 
            "team_stats.total_rebounds", "team_stats.total_steals", "team_stats.total_blocks", 
            "team_stats.total_turnovers", "team_stats.total_fouls", "player.player", "player.points", 
            "player.assists", "player.rebounds", "player.steals", "player.blocks", "player.fouls", 
            "player.turnovers", "quarter", "time", "timestamp")

# Write to Silver Table
df_silver_flat.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/silver/nba_cleaned") \
    .outputMode("append") \
    .format("delta") \
    .toTable("hive_metastore.silver.nba_cleaned")

df_silver_flat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Checking whether data is stored in Silver NBA Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from hive_metastore.silver.nba
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC * Reading from Silver Layer
# MAGIC * Aggregation 
# MAGIC * Email Notification
# MAGIC * Flag is added to send only the new results

# COMMAND ----------

# Aggregation in Gold Layer with Date-based Grouping
df_gold = df_silver_flat \
    .groupBy("date", "gameId", "team", "player") \
    .agg(
        sum("points").alias("total_points"),
        sum("assists").alias("total_assists"),
        sum("rebounds").alias("total_rebounds"),
        sum("steals").alias("total_steals"),
        sum("blocks").alias("total_blocks"),
        sum("turnovers").alias("total_turnovers"),
        sum("fouls").alias("total_fouls"),
        count("eventId").alias("game_count")
    ) \
    .withColumn("flag", lit(0)) \
    .withColumn(
        "performance_rating",
        col("total_points") * 1.5 +
        col("total_assists") * 1.2 +
        col("total_rebounds") * 1.3 +
        col("total_steals") * 2 +
        col("total_blocks") * 2 -
        col("total_turnovers") * 1.5
    )


def send_email(subject, body):
    sender_email = "###########"
    receiver_email = "##############"
    app_password = "tcxd kowt ppge eujr"
    
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email
    
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, app_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Error sending email: {e}")

# Store Top Performer and Best Team
def log_star_performances(batch_df, batch_id):
    top_performer = batch_df.orderBy(col("performance_rating").desc()).limit(1)
    best_team = batch_df.groupBy("team").agg(sum("performance_rating").alias("team_score")).orderBy(col("team_score").desc()).limit(1)
    print(top_performer.schema)
    
    if top_performer.count() > 0 and best_team.count() > 0:
        top_player = top_performer.collect()[0]  # Use `.first()` instead of `.collect()[0]`
        best_team_stats = best_team.collect()[0]

        email_body = f"""
        NBA Best Performance for {top_player['date']}

        🏆 Top Performer: {top_player['player']} ({top_player['team']})
        Performance Rating: {top_player['performance_rating']}

        🔥 Best Team: {best_team_stats['team']}
    
        """


        send_email("NBA Daily Performance Report", email_body)





    # Write Top Performer and Best Team to Gold Tables
    top_performer.write.mode("overwrite").saveAsTable("hive_metastore.gold.nba_top_performer")
    best_team.write.mode("overwrite").saveAsTable("hive_metastore.gold.nba_best_team")

   # Write to a staging table before merging
    batch_df.write.mode("overwrite").saveAsTable("hive_metastore.gold.nba_staging")

    #Join Condition for Flag Updation
    spark.sql("""
    MERGE INTO hive_metastore.gold.nba g
    USING hive_metastore.gold.nba_staging t
    ON g.team = t.team AND g.date = t.date
    WHEN MATCHED THEN UPDATE SET g.flag = 1
    WHEN NOT MATCHED THEN INSERT *
    """)


# Use foreachBatch for Gold Table Writes
df_gold.writeStream \
    .foreachBatch(log_star_performances) \
    .outputMode("complete") \
    .start()

df_gold.display()




# COMMAND ----------

# MAGIC %md
# MAGIC Checking whether data is stored in Gold NBA Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC #Player Performance
# MAGIC select * from hive_metastore.gold.nba_top_performer
# MAGIC #Team Performance
# MAGIC select * from hive_metastore.gold.nba_best_team