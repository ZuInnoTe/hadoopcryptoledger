package org.zuinnote.hadoop.bitcoin.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class BitcoinTransactionElementRecordReaderTest {
    @Test
    public void testRead() throws IOException,InterruptedException {

        BitcoinTransactionElementFileInputFormat format = new BitcoinTransactionElementFileInputFormat();
        Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
        String fileNameBlock = getClass().getClassLoader().getResource("testdata/block176149.blk").getFile();
        Path file = new Path(fileNameBlock);
        FileInputFormat.setInputPaths(job, file);

        List<InputSplit> splits = format.getSplits(job);
	 TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<BytesWritable, BitcoinTransactionElement> recordReader = format.createRecordReader(splits.get(0),context);
	recordReader.initialize(splits.get(0),context);
        
        recordReader.nextKeyValue();
        BytesWritable key = recordReader.getCurrentKey();
	BitcoinTransactionElement element=recordReader.getCurrentValue();

        final String BLOCK_ID = "00000000000007B2A037EAAEFC848E0AD8F41667589F7D5A0ACA3E78C8003AC0";

        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("0000000000000000000000000000000000000000000000000000000000000000", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(0, element.getType());
        Assert.assertEquals(0l, element.getAmount());

        
        recordReader.nextKeyValue();
        key = recordReader.getCurrentKey();
	element=recordReader.getCurrentValue();
        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("3C8335377E4A59B96D2FA621F7F5DE7901FAC6D7FD0BFB965AE498F39196ADD2", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(1, element.getType());
        Assert.assertEquals(5005901373l, element.getAmount());

        //first input of second transaction
        
        recordReader.nextKeyValue();
        key = recordReader.getCurrentKey();
	element=recordReader.getCurrentValue();
        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("17E9952BB93FCFF1648EA50D8F8273FB884AA0D55C1CDEF1B5B863071C99BCA6", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(0, element.getType());
        Assert.assertEquals(0, element.getAmount());

        recordReader.nextKeyValue();
        
        recordReader.nextKeyValue();
        key = recordReader.getCurrentKey();
	element=recordReader.getCurrentValue();
        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("EF6EDCB7E849BA29656F4C8E333512467768C05A65E42A68ADB02832D33CFC97", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(1, element.getType());
        Assert.assertEquals(18941270000l, element.getAmount());

        while (recordReader.nextKeyValue()) {
            key = recordReader.getCurrentKey();
	    element=recordReader.getCurrentValue();
            Assert.assertNotNull(element.getBlockHash());
            Assert.assertNotNull(element.getTransactionHash());
        }

    }

   
}
