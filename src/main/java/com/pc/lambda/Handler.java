package com.pc.lambda;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class Handler implements RequestHandler<String, String> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String handleRequest(String event, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("event => " + gson.toJson(event) + "\n");
		logger.log("context => " + gson.toJson(context) + "\n");

		ResponseInputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		//XSSFWorkbook workBook = null;

		try {
			String key = System.getenv("fileKey");
			String fileName = System.getenv("fileName");
			String bucketName = "testbucketcoolduck";

			S3Client s3Client = S3Client.builder().region(Region.AP_SOUTH_1).build();

			ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
			ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest);
			listBucketsResponse.buckets().stream().forEach(x -> logger.log("Object Name: " + x.name()));

			Path path = Paths.get(fileName);
			Date date = new Date();

			logger.log("Getting S3 object...");

			GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
			// inputStream = s3Client.getObject(getObjectRequest);
			GetObjectResponse response = s3Client.getObject(getObjectRequest, path);			

			long diff = new Date().getTime() - date.getTime();
			logger.log(MessageFormat.format("Time taken to download the file: {0} seconds ",
					(TimeUnit.SECONDS.convert(diff, TimeUnit.MILLISECONDS))));
			// logger.log("S3 Object Input Stream: " + inputStream.toString());

			File file = path.toFile();
			file.getAbsoluteFile();
			logger.log(MessageFormat.format("File absolute path: {0} and canonical path: {1}.", file.getAbsolutePath(), file.getCanonicalPath()));

			/*
			 * outputStream = new ByteArrayOutputStream();
			 * 
			 * date = new Date(); byte[] buf = new byte[1024]; int n = 0; while (-1 != (n =
			 * inputStream.read(buf))) { outputStream.write(buf, 0, n); } diff = new
			 * Date().getTime() - date.getTime(); logger.log(MessageFormat.
			 * format("Time taken to convert the file into stream: {0} seconds ",
			 * (TimeUnit.SECONDS.convert(diff, TimeUnit.MILLISECONDS))));
			 * 
			 * outputStream.close(); inputStream.close();
			 * 
			 * logger.log("Writing outputStream to fileOutputStream..."); byte[] response =
			 * outputStream.toByteArray(); fileOutputStream = new FileOutputStream("/tmp/" +
			 * fileName); fileOutputStream.write(response); fileOutputStream.close();
			 * logger.log("Written outputStream to fileOutputStream !!");
			 * 
			 * logger.log("Writing fileOutputStream to excel file..."); File file = new
			 * File("/tmp/" + fileName); fileInputStream = new FileInputStream(file);
			 * workBook = new XSSFWorkbook(fileInputStream); XSSFSheet workBookSheet =
			 * workBook.getSheet(workBook.getSheetName(0));
			 * logger.log("Written fileOutputStream to excel file: " +
			 * file.getAbsolutePath());
			 * 
			 * int rowCount = workBookSheet.getPhysicalNumberOfRows();
			 * logger.log("Row Count: " + rowCount + "\n");
			 */ } catch (S3Exception ace) {
			logger.log("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Simple Storage Service, such as not "
					+ "being able to access the network.");
			logger.log("Error Message: " + ace.getMessage());
		} catch (AwsServiceException ase) {
			logger.log("Caught an AmazonServiceException, which means"
					+ " your request made it to Simple Storage Service, but was"
					+ " rejected with an error response for some reason.");
			logger.log("Error Message:    " + ase.getMessage());

		} catch (IOException ioe) {
			logger.log(
					"Caught an IOException, which means " + "the client encountered a serious internal problem while "
							+ "performing IO operations on the file");
			logger.log("Error Message: " + ioe.getMessage());

		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
				if (outputStream != null)
					outputStream.close();
				if (fileInputStream != null)
					fileInputStream.close();
				if (fileOutputStream != null)
					fileOutputStream.close();
//				if (workBook != null)
//					workBook.close();
			} catch (IOException ioe) {
				logger.log("Caught an IOException while closing the handles.");
				logger.log("Error Message: " + ioe.getMessage());
			}
		}

		logger.log("===========================================\n");
		logger.log("Execution Ended for Simple Storage Services\n");
		logger.log("===========================================\n");

		return "Processed successfully";
	}
}
