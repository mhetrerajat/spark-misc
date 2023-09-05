"""
Session Type Duration
Twitch

https://platform.stratascratch.com/coding/2011-session-type-duration?code_type=6

Calculate the average session duration for each session type?

Tables

twitch_sessions
user_id: int
session_start: datetime
session_end: datetime
session_id: int
session_type: varchar
"""

# Import your libraries
import pyspark
from pyspark.sql.functions import col, avg

# Start writing code
twitch_sessions = (
    twitch_sessions.withColumn("diff", col("session_end") - col("session_start"))
    .groupBy("session_type")
    .agg(avg("diff").alias("avg(duration)"))
)

# To validate your solution, convert your final pySpark df to a pandas df
twitch_sessions.toPandas()
