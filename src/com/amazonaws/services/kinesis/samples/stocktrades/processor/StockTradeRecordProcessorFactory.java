/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * Used to create new stock trade record processors.
 *
 */
public class StockTradeRecordProcessorFactory implements ShardRecordProcessorFactory {

    /**
     * Constructor.
     */
    public StockTradeRecordProcessorFactory() {
    	super();
        
    }
	@Override
	public ShardRecordProcessor shardRecordProcessor() {
		// TODO Auto-generated method stub
		//createProcessor();
		System.out.println("start");
		return new StockTradeRecordProcessor();
	}

}
