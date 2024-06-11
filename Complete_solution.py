from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,StructType,StructField
from pyspark.sql.functions import col,substring,cast,explode,split,avg,round
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Creating spark session
spark=SparkSession.builder.appName('Movie rating').getOrCreate()

#Schema for movies file
movieSchema=StructType([
    StructField('movieid',IntegerType()),
    StructField('movieName',StringType()),
    StructField('genre',StringType())
])

#Create DF from movies file and add column for year extracted form movie name
try:
    movieDF = spark.read.csv(r"D:\GFK Interview task\Src files\movies.dat",header=False, schema=movieSchema, sep='::')\
        .withColumn('Year', substring(col('movieName'), -5, 4).cast('int'))

    logger.info("Movies DataFrame loaded and Year column added.")
except Exception as e:
    logger.error(f"Error loading movies file: {e}")


#filter year>1989 and select only required columns
movieDFYearFilter=movieDF.filter(col('Year')>1989).select('movieid','genre','Year')

#replace already exsting genre column to contain array of genres
movieDFsplit=movieDFYearFilter.withColumn('genre',split(movieDFYearFilter['genre'],'\|'))

#Explode genre column to create multiple rows for sigle element in array
movieDFfinal=movieDFsplit.withColumn('genre',explode(col('genre')))


#Schema for users file
userSchema=StructType([
    StructField('userid',IntegerType()),
    StructField('gender',StringType()),
    StructField('age_group',IntegerType()),
    StructField('occupation_id',IntegerType()),
    StructField('zip_code',IntegerType())
])


#Create df from users file. filter for required age group and select required columns
try:
    usersDF=spark.read.csv(r'D:\GFK Interview task\Src files\users.dat',header=False,schema=userSchema,sep='::')\
        .filter((col('age_group')>1) & (col('age_group')<50))\
        .select('userid','age_group')
    logger.info("Users DataFrame loaded and filtered.")
except Exception as e:
    logger.error(f"Error loading users file: {e}")

#schema for ratings file
ratingSchema=StructType([
    StructField('userID',IntegerType()),
    StructField('movieID',IntegerType()),
    StructField('rating',IntegerType()),
    StructField('timestamp',IntegerType()),
])

#Creating DF from ratings file and selecting only required columns
try:
    ratingsDF=spark.read.csv(r'D:\GFK Interview task\Src files\ratings.dat',header=False,sep='::',schema=ratingSchema)\
        .select('userID','movieID','rating')
    logger.info("Ratings DataFrame loaded.")
except Exception as e:
    logger.error(f"Error loading ratings file: {e}")

#Join ratings and users data based on userid and drop columns that are not required for future references
joinConditionone=ratingsDF.userID==usersDF.userid
ratingUser=usersDF.join(ratingsDF,joinConditionone,'inner').drop(*['userid','age_group'])

#Join above created DF with movies DF and drop columns that are not required for future references
joinCondition2=ratingUser.movieID==movieDFfinal.movieid
movieRating=movieDFfinal.join(ratingUser,joinCondition2,'inner').drop(*['movieid','movieID'])

#Calculate average ratings based on genre and year. Average is rounded of by 4 digits and result ordered by Year
try:
    avgRatings=movieRating.groupBy('genre','Year').agg(round(avg('rating'),4).alias('avg_rating')).orderBy('Year').show(50)
    logger.info("Average ratings calculated and displayed.")
except Exception as e:
    logger.error(f"Error calculating average ratings: {e}")


#Stop spark session
spark.stop()



