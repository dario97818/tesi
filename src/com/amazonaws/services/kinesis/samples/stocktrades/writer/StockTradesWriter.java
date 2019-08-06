/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.stocktrades.writer;

import java.awt.List;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.producer.KinesisProducer;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.kinesis.producer.protobuf.Messages.PutRecordResult;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.CredentialUtils;

/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class StockTradesWriter {

    public static void main(String[] args) throws Exception {
        
       
        sendStockTrade2();
       
    }
    private static void sendStockTrade2() throws Exception {
    	   AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
           
    	   clientBuilder.setRegion("us-east-2");
           clientBuilder.setCredentials(CredentialUtils.getCredentialsProvider());
           clientBuilder.setClientConfiguration(ConfigurationUtils.getClientConfigWithUserAgent());
           AmazonKinesis kinesisClient = clientBuilder.build();
           StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();
           System.out.println("scrivo");
           while(true) {
        	   
        	   StockTrade trade = stockTradeGenerator.getRandomTrade();
        	   byte[] bytes = trade.toJsonAsBytes();
         	  ByteBuffer data= ByteBuffer.wrap(bytes);
       
               String TIMESTAMP = Long.toString(System.currentTimeMillis());
              
               
               PutRecordRequest a=new PutRecordRequest();
               a.setData(data);
               a.setPartitionKey(TIMESTAMP);
               a.setStreamName("StockTradeStream");
               //System.out.println(trade.toString());
               com.amazonaws.services.kinesis.model.PutRecordResult putRecordResult =kinesisClient.putRecord(a);
              // System.out.println("Put Result" + putRecordResult);
           }

    	  
    }
    

}
