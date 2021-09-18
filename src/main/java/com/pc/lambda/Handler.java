package com.pc.lambda;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Handler implements RequestHandler<String, String> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String handleRequest(String event, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("event => " + gson.toJson(event) + "\n");
		logger.log("context => " + gson.toJson(context) + "\n");

		InputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		XSSFWorkbook workBook = null;

		try {
			String key = System.getenv("fileKey");
			String fileName = "Download.xlsx";
			String bucketName = "testbucketcoolduck";

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.AP_SOUTH_1).build();
			ObjectListing objects = s3Client.listObjects(bucketName);
			logger.log("No. of Objects: " + objects.getObjectSummaries().size());

			Date date = new Date();
			S3Object fileObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
			inputStream = new BufferedInputStream(fileObject.getObjectContent());
			logger.log("Input Stream: " + inputStream.toString());
			
			long diff = new Date().getTime() - date.getTime();
			logger.log(MessageFormat.format("Time taken to download the file: {0} seconds ", 
					(TimeUnit.SECONDS.convert(diff, TimeUnit.MILLISECONDS))));
			
			outputStream = new ByteArrayOutputStream();

			date = new Date();
			byte[] buf = new byte[1024];
			int n = 0;
			while (-1 != (n = inputStream.read(buf))) {
				outputStream.write(buf, 0, n);
			}
			diff = new Date().getTime() - date.getTime();
			logger.log(MessageFormat.format("Time taken to convert the file into stream: {0} seconds ", 
					(TimeUnit.SECONDS.convert(diff, TimeUnit.MILLISECONDS))));
			
			outputStream.close();
			inputStream.close();

			logger.log("Writing outputStream to fileOutputStream...");
			byte[] response = outputStream.toByteArray();
			fileOutputStream = new FileOutputStream("/tmp/" + fileName);
			fileOutputStream.write(response);
			fileOutputStream.close();
			logger.log("Written outputStream to fileOutputStream !!");
			
			logger.log("Writing fileOutputStream to excel file...");
			File file = new File("/tmp/" + fileName);
			fileInputStream = new FileInputStream(file);
			workBook = new XSSFWorkbook(fileInputStream);
			XSSFSheet workBookSheet = workBook.getSheet(workBook.getSheetName(0));
			logger.log("Written fileOutputStream to excel file: " + file.getAbsolutePath());
			
			int rowCount = workBookSheet.getPhysicalNumberOfRows();
			logger.log("Row Count: " + rowCount + "\n");
		} catch (AmazonServiceException ase) {
			logger.log("Caught an AmazonServiceException, which means"
					+ " your request made it to Simple Storage Service, but was"
					+ " rejected with an error response for some reason.");
			logger.log("Error Message:    " + ase.getMessage());
			logger.log("HTTP Status Code: " + ase.getStatusCode());
			logger.log("AWS Error Code:   " + ase.getErrorCode());
			logger.log("Error Type:       " + ase.getErrorType());
			logger.log("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Simple Storage Service, such as not "
					+ "being able to access the network.");
			logger.log("Error Message: " + ace.getMessage());
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
				if (workBook != null)
					workBook.close();
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
