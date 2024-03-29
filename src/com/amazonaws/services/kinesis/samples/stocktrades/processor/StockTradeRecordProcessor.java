/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;

import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class StockTradeRecordProcessor implements ShardRecordProcessor {

    private static final Log LOG = LogFactory.getLog(StockTradeRecordProcessor.class);
    private String kinesisShardId;
    // Reporting interval
    private static final long REPORTING_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextReportingTimeInMillis;
    // Checkpointing interval
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    // Aggregates stats for stock trades
    private StockStats stockStats = new StockStats();

    /**
     *      * {@inheritDoc}
     */
    public void initializeV(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    /**
     * {@inheritDoc}
     * @throws InterruptedException 
     */
    public void processRecordsV(List<Record> list, RecordProcessorCheckpointer recordProcessorCheckpointer) throws InterruptedException {
    	//System.out.println("start2");
    	for (Record record : list) {
            // process record
    		
            processRecord(record);
        }

        // If it is time to report stats as per the reporting interval, report stats
    	
        if (System.currentTimeMillis() >= nextReportingTimeInMillis) {
           if( reportStats()!=1) {
        	  // Thread.sleep(10000);
           }
           else
           {
            resetStats();
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;}
        }

        // Checkpoint once every checkpoint interval
        /*if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(recordProcessorCheckpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }*/
    }

    private int reportStats() {
    	System.out.println("startR");
        System.out.println("****** Shard " + kinesisShardId + " stats for last 1 minute ******\n" +
                stockStats + "\n" +
                "****************************************************************\n");
        return 1;
    }

    private void resetStats() {
        stockStats = new StockStats();
    }

    private void processRecord(Record record) {
        StockTrade trade = StockTrade.fromJsonAsBytes(record.getData().array());
        if (trade == null) {
        	System.out.println("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.getPartitionKey());
            LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.getPartitionKey());
            return;
        }
        stockStats.addStockTrade(trade);
    }

    /**
     * {@inheritDoc}
     */
    public void shutdown(RecordProcessorCheckpointer checkpointer) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        
            checkpoint(checkpointer);
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
    	System.out.println("startck");
        LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

	@Override
	public void initialize(InitializationInput initializationInput) {
		// TODO Auto-generated method stub
		System.out.println("start");
		initializeV(initializationInput.shardId());
		
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		// TODO Auto-generated method stub
		System.out.println("startrcc");
		 //processRecordsV(processRecordsInput.records(), processRecordsInput.checkpointer());
		
	}

	@Override
	public void leaseLost(LeaseLostInput leaseLostInput) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shardEnded(ShardEndedInput shardEndedInput) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
		// TODO Auto-generated method stub
		shutdown(shutdownRequestedInput.checkpointer());
	}

}
