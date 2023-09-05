"""
New And Existing Users

IBM
Apple
Microsoft

Calculate the share of new and existing users for each month in the table. Output the month, share of new users, and share of existing users as a ratio.
New users are defined as users who started using services in the current month (there is no usage history in previous months). Existing users are users who used services in current month, but they also used services in any previous month.
Assume that the dates are all from the year 2020.
HINT: Users are contained in user_id column


Tables

fact_events
id: int
time_id: datetime
user_id: varchar
customer_id: varchar
client_id: varchar
event_type: varchar
event_id: int

"""
# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code
result = (
    fact_events.withColumn("month", F.month(F.to_date(F.col("time_id"))))
    .groupBy("month")
    .agg(F.countDistinct("user_id").alias("n_users"))
    .withColumnRenamed("month", "month_min")
)

df1 = (
    fact_events.groupBy("user_id")
    .agg(F.min("time_id").alias("month_min"))
    .withColumn("month_min", F.month(F.to_date(F.col("month_min"))))
    .groupBy("month_min")
    .agg(F.countDistinct("user_id").alias("n_new_users"))
)

result = (
    result.join(df1, on="month_min", how="left")
    .withColumn("share_of_new_users", F.col("n_new_users") / F.col("n_users"))
    .withColumn("share_of_old_users", 1 - F.col("share_of_new_users"))
    .select("month_min", "share_of_new_users", "share_of_old_users")
    .orderBy("month_min")
)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
