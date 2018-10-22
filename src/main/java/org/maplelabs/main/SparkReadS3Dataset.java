/*
 * Copyright (c) 2018 MapleLabs. All Rights Reserved.
 */

package org.maplelabs.main;

import org.apache.log4j.Logger;
import org.apache.hadoop.security.AccessControlException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * Reads the dataset from AWS S3 bucket
 */
public class SparkReadS3Dataset {
    private static final Logger LOGGER = Logger.getLogger(SparkReadS3Dataset.class);
    public static void main(String[] args) throws SecurityException, IOException {
        if (args.length != 2) {
	    LOGGER.error("Please provide the correct number of arguments...");
            System.exit(1);
        }
        SparkSession spark = SparkSession.builder().appName("Spark_S3")
                                                   .config("fs.s3n.awsAccessKeyId", args[0])
                                                   .config("fs.s3n.awsSecretAccessKey", args[1])
                                                   .getOrCreate();
        String gnrlPayPath = "s3n://maplelabs/maple/OP_DTL_GNRL_PAY.csv";
        String rsrchPayPath = "s3n://maplelabs/maple/OP_DTL_RSRCH_PAY.csv";
        try {
            LOGGER.info("Reading the csv file" + gnrlPayPath + "from S3 bucket");
	    Dataset<Row> s3aRdd1 = spark.read().format("csv")
                                               .option("header", "true")
                                               .option("treatEmptyValuesAsNulls", "true")
		                               .option("nullValue", "0")
		                               .option("sep", ",")
		                               .csv(gnrlPayPath);
	    if (s3aRdd1.count() > 0) {
	        LOGGER.info("Number of records in the given file is " + s3aRdd1.count());
		s3aRdd1.createOrReplaceTempView("temp_table");
		s3aRdd1.createOrReplaceTempView("gnrl_pay_orc");
		String query1 = "select count(physician_profile_id) as no_of_physicians,Recipient_Zip_" +
                                "Code from temp_table group by Recipient_Zip_Code";
                String query2 = "select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name," +
                                "Physician_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_i" +
                                "nvestment from gnrl_pay_orc group by Applicable_Manufacturer_or_Appli" +
                                "cable_GPO_Making_Payment_Name,Physician_Specialty";
		Dataset<Row> df1 = spark.sql(query1) ;
		df1.show();
                Dataset<Row> df2 = spark.sql(query2) ;
                df2.show();
		LOGGER.info("Done");
	    } else {
		LOGGER.error("No records found in " + gnrlPayPath);
                System.exit(1);
	    }
            LOGGER.info("Reading the csv file" + rsrchPayPath + "from S3 bucket");
            Dataset<Row> s3aRdd2 = spark.read().format("csv")
                                               .option("header", "true")
                                               .option("treatEmptyValuesAsNulls", "true")
                                               .option("nullValue", "0")
                                               .option("sep", ",")
                                               .csv(rsrchPayPath);
            if (s3aRdd2.count() > 0) {
                LOGGER.info("Number of records in the given file is " + s3aRdd2.count());
                s3aRdd2.createOrReplaceTempView("rsrch_pay_orc");
                String query3 = "select Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Prin" +
                                "cipal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Pri" +
                                "ncipal_Investigator_4_Specialty,Principal_Investigator_5_Specialty,su" +
                                "m(Total_Amount_of_Payment_USDollars) as total_investment from rsrch_p" +
                                "ay_orc group by Teaching_Hospital_Name,Principal_Investigator_1_Speci" +
                                "alty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Spec" +
                                "ialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Spe" +
                                "cialty";
                Dataset<Row> df3 = spark.sql(query3) ;
                df3.show();
                LOGGER.info("Done");
            } else {
                LOGGER.error("No records found in " + rsrchPayPath);
                System.exit(1);
            }
        } catch (Exception ex) {
            LOGGER.error(ex.toString());
            System.exit(1);
        }
    }
}
