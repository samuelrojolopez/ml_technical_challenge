from os.path import abspath
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, current_date, when, lit, col, \
    date_add, months_between, floor

selected_columns = ['id',
                    'age',
                    'years_on_the_job',
                    'nb_previous_loans',
                    'avg_amount_loans_previous',
                    'flag_own_car',
                    'status']

warehouse_location = abspath('spark-warehouse')


def order_by_id_and_loan_date(data_frame):
    """
    Order the dataframe by id and loan_date, while adding an incremental
    index on the ordered dataframe

    :param data_frame: Spark Dataframe containing the columns id and loan_date
    :return: Spark DataFrame
    """
    data_frame = data_frame.orderBy("id", "loan_date")
    return data_frame


def cast_dates_to_datetime(data_frame):
    """
    Casts a Spark DataFrame column with string dates values into date type
    values and turns into nulls all the dates greater than the last run date,
    plus one day in order to consider any time zone difference and avoid valid
    data loss.

    :param data_frame: Spark DataFrame containing the column loan_date
    :return: Spark DataFrame
    """
    # loan_date
    data_frame = data_frame.withColumn('loan_date',
                                       data_frame['loan_date'].cast(
                                           'date'))
    data_frame = data_frame.withColumn('loan_date',
                                       when(col("loan_date")
                                            <= date_add(current_date(), 1),
                                            col("loan_date")))
    # birthday
    data_frame = data_frame.withColumn('birthday',
                                       data_frame['birthday'].cast('date'))
    data_frame = data_frame.withColumn('birthday',
                                       when(col("birthday")
                                            <= date_add(current_date(), 1),
                                            col("birthday")))
    # job_start_date
    data_frame = data_frame.withColumn('job_start_date',
                                       data_frame['job_start_date'].cast(
                                           'date'))
    data_frame = data_frame.withColumn('job_start_date',
                                       when(col("job_start_date")
                                            <= date_add(current_date(), 1),
                                            col("job_start_date")))
    return data_frame


def generate_nb_previous_loans(data_frame):
    """
    Calculates a new column field from the fields id and loan date, containing
    the number of previous loans autorized to the id for each new loan.

    :param data_frame: Spark DataFrame that contains the fields id and
    loan_date, requiring the date in a format that can be ordered.
    :return: Spark DataFrame with new column nb_previous_loans
    """
    window = Window.partitionBy("id").orderBy("loan_date")
    data_frame = data_frame.withColumn("nb_previous_loans",
                                       rank().over(window) - 1)
    return data_frame


def generate_avg_amount_loans_previous(data_frame):
    """
    Calculates a new column field from the fields id and loan date, containing
    the average amount of previous loans autorized to the id for each new
    loan.

    :param data_frame: Spark DataFrame that contains the fields id and
    loan_date, requiring the loan_date ordered.
    :return: Spark DataFrame with new column nb_previous_loans
    """
    window = Window.partitionBy("id"). \
        rowsBetween(Window.unboundedPreceding, -1)

    data_frame = data_frame.sort(f.asc('loan_date')) \
        .withColumn("avg_amount_loans_previous",
                    f.mean('loan_amount').over(window))
    return data_frame


def compute_age_features(data_frame):
    """
    Compute the age for each row by the distance between the current date()
    and the birthday date contained in the birthday field.

    :param data_frame: Spark DataFrame that contains the field birthday.
    :return: Spark DataFrame with new column age
    """
    data_frame = data_frame \
        .withColumn("age",
                    floor(months_between(current_date(),
                                         col("birthday")) / lit(12))
                    .cast('integer'))
    return data_frame


def compute_years_on_the_job(data_frame):
    """
    Compute the years worked sinche a start date for each row by the distance
    between the current date() and the start job date contained in the
    job_start_date field.

    :param data_frame: Spark DataFrame that contains the field job_start_date.
    :return: Spark DataFrame with new column years_on_the_job
    """
    data_frame = data_frame \
        .withColumn("years_on_the_job",
                    floor(months_between(current_date(),
                                         col("job_start_date")) / lit(12))
                    .cast('integer'))
    return data_frame


def add_feature_own_car_flag(data_frame):
    """
    Decode the field flag_own_car from binary valies Y/N into cardinal values
    1/0.

    :param data_frame: Spark DataFrame that contains the field flag_own_car.
    :return: Spark DataFrame with decoded field flag_own_car
    """
    data_frame = data_frame.withColumn('flag_own_car',
                                       when(col("flag_own_car")
                                            == 'N', lit(1)).otherwise(0))
    return data_frame


def credit_fe_columns_subset(data_frame, columns):
    """
    Subset of the dataset with as selected subset of fields.

    :param data_frame:  Spark DataFrame
    :return:  Spark DataFrame with only the subset of columns
    """
    data_frame = data_frame.select(columns)
    return data_frame


# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Credit Features Processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file from the HDFS
credit_raw_df = spark.read.csv("/dataset_credit_risk/dataset_credit_risk.csv",
                               header=True,
                               sep=',',
                               inferSchema=True)


data_frame = order_by_id_and_loan_date(credit_raw_df)
data_frame = cast_dates_to_datetime(data_frame)
data_frame = generate_nb_previous_loans(data_frame)
data_frame = generate_avg_amount_loans_previous(data_frame)
data_frame = compute_age_features(data_frame)
data_frame = compute_years_on_the_job(data_frame)
data_frame = add_feature_own_car_flag(data_frame)
processed_credit_df = credit_fe_columns_subset(data_frame,
                                               columns=selected_columns)

# Write results to parquet
processed_credit_df.write.mode("overwrite").insertInto("credit_fraud_features")

# Write the results to hive
processed_credit_df.write.mode("append")\
    .parquet("/opt/spark-data/credit_fraud_features.csv")
