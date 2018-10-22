import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads the dataset from AWS S3 bucket
 */
public class SparkReadS3Dataset {
	public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("Spark_S3").config("fs.s3n.awsAccessKeyId", "AKIAIRVI4GXD25P6FPXQ").config("fs.s3n.awsSecretAccessKey", "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").getOrCreate();
		 Dataset<Row> s3aRdd = spark.read().option("header", "true").option("treatEmptyValuesAsNulls", "true")
	                .option("nullValue", "0").option("delimiter", ",")
	                .csv("s3n://AKIAIRVI4GXD25P6FPXQ:7FT8XeSCoXmlyhXioSHvlmATIk1uOvTRsZjDkLty@rundeckpndacontainer/rupa/OP_DTL_GNRL_PAY.csv");
		 System.out.println("***********************"+s3aRdd.count()+"************************");
		 s3aRdd.createOrReplaceTempView("temp_table");

		 Dataset<Row> df1=spark.sql("select count(physician_profile_id) as no_of_physicians ,Recipient_Zip_Code from temp_table group by Recipient_Zip_Code") ;
		 df1.show();
	 }
}
