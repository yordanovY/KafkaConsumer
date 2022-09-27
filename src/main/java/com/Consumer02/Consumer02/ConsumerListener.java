package com.Consumer02.Consumer02;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

@Service
public class ConsumerListener {
	
	private static final Logger logger = LogManager.getLogger(ConsumerListener.class);
	
	@RetryableTopic(attempts = "2", autoCreateTopics = "false", dltStrategy = DltStrategy.FAIL_ON_ERROR , dltTopicSuffix = ".dlq")
	@KafkaListener(topics = "Topic03", groupId = "group01")
	public void listenGroupFoo(ConsumerRecord<String, String> message) {
		//Time Stamp to be used later
		String timeStamp = DateFormat.YEAR_FIELD + "_" + DateFormat.MONTH_FIELD + "_" +  DateFormat.DAY_OF_WEEK_FIELD + "_" +  DateFormat.HOUR_OF_DAY0_FIELD + "_" +  DateFormat.MILLISECOND_FIELD;
		//File names for archiving
		String consumedFileName = "message_" + timeStamp;
		String consumedFileMetadata = "metadata_" + timeStamp;
		File consumedFile = new File( "ConsumedMessage//" + consumedFileName + ".txt");
		File consumedFileMetaData = new File( "ConsumedMessage//" + consumedFileMetadata + ".txt");
		//header
		String headerString = "";
		for(Header h : message.headers()) {
			headerString += h.key() + "=" + h.value().toString() + "\n";
		}
		headerString += "topic = " + message.topic();
		headerString += "Consumed at = " + timeStamp;
		logger.info("Processing message " + timeStamp + ".");
		//write to consumed file
	    try {
	        FileWriter myWriter = new FileWriter(consumedFile);
	        myWriter.write(message.value());
	        myWriter.close();
	      } catch (IOException e) {
	    	logger.info("An error occurred in generating file" + consumedFileName + ".");
	        e.printStackTrace();
	      }
		//write to metadata file 
	    try {
	        FileWriter myWriter = new FileWriter(consumedFileMetaData);
	        myWriter.write(headerString);
	        myWriter.close();
	      } catch (IOException e) {
	        logger.info("An error occurred in generating metadata file" + consumedFileMetadata + ".");
	        e.printStackTrace();
	      }
		logger.info("Finished processing message " + timeStamp + ".");
	}
	
	@DltHandler
	public void dltConsumer(ConsumerRecord<String, String> message) {
		String timeStamp = DateFormat.YEAR_FIELD + "_" + DateFormat.MONTH_FIELD + "_" +  DateFormat.DAY_OF_WEEK_FIELD + "_" +  DateFormat.HOUR_OF_DAY0_FIELD + "_" +  DateFormat.MILLISECOND_FIELD;
	logger.info("Issues at message " + timeStamp + ".");
	message.headers().add(new RecordHeader(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE,message.key().toString().getBytes()));
	message.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE,message.value().toString().getBytes()));
	message.headers().add(new RecordHeader(KafkaHeaders.ORIGINAL_OFFSET, String.valueOf(message.offset()).getBytes()));
	message.headers().add(new RecordHeader(KafkaHeaders.ORIGINAL_TIMESTAMP,timeStamp.getBytes()));
	
	 String errprFile = "error_" + timeStamp;
	 File consumedFile = new File( "ErrorFiles//" + errprFile + ".txt");
	    try {
	        FileWriter myWriter = new FileWriter(consumedFile);
	        myWriter.write(message.value());
	        myWriter.close();
	        logger.info("Successfully output kafka file " + errprFile + ".");
	      } catch (IOException e) {
	    	logger.info("An error occurred in generating file" + errprFile + ".");
	        e.printStackTrace();
	      }
	}
}
