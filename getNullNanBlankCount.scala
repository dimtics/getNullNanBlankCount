// import packages 
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer


def getNullNanBlankCount()(df: DataFrame): DataFrame = {
  
   // get all the column names into an array
  val allColsArr = df.columns
  
  // create an empty array buffer to temporarily store column names and count values
  val arrBuffer = ArrayBuffer[(String, (Long, Long, Long))]()
  
  // iterate the columns array, select each column, filter and count its null, nan and blank values
  for(field <- allColsArr) {   
    // count nulls
    val nullCondition = col(field).isNull 
    val nullCount = df.select(col(field)).filter(nullCondition).count()  
    
    // count nans
    val nanCondition = col(field).isNaN
    val nanCount = df.select(col(field)).filter(nanCondition).count()  
    
    // count blank/empty values
    val blankCount = df.select(col(field)).filter(col(field) === " ").count()  
     
  // append column names and count values to the array buffer
    arrBuffer.append((field, (nullCount, nanCount, blankCount)))
  }
   
  // convert the array buffer into a new dataframe
  val resDf = spark.createDataFrame(arrBuffer).select(monotonically_increasing_id().alias("id"), col("_1").alias("table_column_name"), col("_2").getField("_1").alias("null_count"), col("_2").getField("_2").alias("nan_count"), col("_2").getField("_3").alias("blank_count"))
 
  // returns the final dataframe with null, nan and blank count values
  resDf
}