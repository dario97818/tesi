/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.flink.shaded.akka.org.jboss.netty.channel.socket.Worker;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import software.amazon.awssdk.regions.Region;
import com.amazonaws.regions.RegionUtils;


import com.amazonaws.services.kinesis.samples.stocktrades.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.CredentialUtils;
import com.amazonaws.util.IOUtils;



import java.nio.charset.StandardCharsets;

import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
/**
 * Uses the Kinesis Client Library (KCL) to continuously consume and process stock trade
 * records from the stock trades stream. KCL monitors the number of shards and creates
 * record processor instances to read and process records from each shard. KCL also
 * load balances shards across all the instances of this processor.
 *
 */
public class StockTradesProcessor {

     
  

    public static void main(String[] args) throws Exception {
    
        
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        
 	   clientBuilder.setRegion("us-east-2");
        clientBuilder.setCredentials(CredentialUtils.getCredentialsProvider());
        clientBuilder.setClientConfiguration(ConfigurationUtils.getClientConfigWithUserAgent());
        AmazonKinesis kinesisClient = clientBuilder.build();
        GetRecordsRequest req=new GetRecordsRequest();
        GetShardIteratorRequest sreq=new GetShardIteratorRequest();
        sreq.setShardId("shardId-000000000000");
        sreq.setShardIteratorType("AT_TIMESTAMP");
        Date t=new Date(System.currentTimeMillis());
        sreq.withTimestamp(t);
        sreq.setStreamName("StockTradeStream");
        req.setShardIterator(kinesisClient.getShardIterator(sreq).getShardIterator());
        System.out.println("leggo");
        
        StockTradeRecordProcessor pr=new StockTradeRecordProcessor();
        pr.initializeV("shardId-000000000000");
        while(true) {
        
       
       
     	
      
        GetRecordsResult a=kinesisClient.getRecords(req);
       
       List<Record> r= a.getRecords();
       //System.out.println(a.toString());
       if(r.size()!=0) {
    	  pr.processRecordsV(r, null);
    	  
    	   /*
    	   for(int i=0;i<r.size();i++) {
     
    		   String s=new String(r.get(i).getData().array());
    		   System.out.println(s);
    		   }*/
    	   }
       req.setShardIterator(a.getNextShardIterator());
        }
      
        
       
       
        
    }

}
