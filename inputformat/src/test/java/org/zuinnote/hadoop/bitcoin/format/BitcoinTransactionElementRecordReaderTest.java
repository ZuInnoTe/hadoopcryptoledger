package org.zuinnote.hadoop.bitcoin.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class BitcoinTransactionElementRecordReaderTest {
    @Test
    public void testRead() throws IOException {

        BitcoinTransactionElementFileInputFormat format = new BitcoinTransactionElementFileInputFormat();
        JobConf conf = new JobConf();
        format.configure(conf);
        String fileNameBlock = getClass().getClassLoader().getResource("testdata/block176149.blk").getFile();
        Path file = new Path(fileNameBlock);
        FileInputFormat.setInputPaths(conf, file);

        InputSplit[] splits = format.getSplits(conf, 1);

        RecordReader<BytesWritable, BitcoinTransactionElement> recordReader = format.getRecordReader(splits[0], conf, Reporter.NULL);

        BytesWritable key = new BytesWritable();
        BitcoinTransactionElement element = new BitcoinTransactionElement();

        recordReader.next(key, element);

        final String BLOCK_ID = "00000000000007B2A037EAAEFC848E0AD8F41667589F7D5A0ACA3E78C8003AC0";

        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("0000000000000000000000000000000000000000000000000000000000000000", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(0, element.getType());
        Assert.assertEquals(0l, element.getAmount());

        recordReader.next(key, element);
        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("3C8335377E4A59B96D2FA621F7F5DE7901FAC6D7FD0BFB965AE498F39196ADD2", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(1, element.getType());
        Assert.assertEquals(5005901373l, element.getAmount());

        //first input of second transaction
        recordReader.next(key, element);
        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("17E9952BB93FCFF1648EA50D8F8273FB884AA0D55C1CDEF1B5B863071C99BCA6", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(0, element.getType());
        Assert.assertEquals(0, element.getAmount());

        recordReader.next(key, element);

        recordReader.next(key, element);
        Assert.assertEquals(BLOCK_ID, BitcoinUtil.convertByteArrayToHexString(element.getBlockHash()));
        Assert.assertEquals("EF6EDCB7E849BA29656F4C8E333512467768C05A65E42A68ADB02832D33CFC97", BitcoinUtil.convertByteArrayToHexString(element.getTransactionHash()));
        Assert.assertEquals(1, element.getType());
        Assert.assertEquals(18941270000l, element.getAmount());

        while (recordReader.next(key, element)) {
            Assert.assertNotNull(element.getBlockHash());
            Assert.assertNotNull(element.getTransactionHash());
        }

    }

   
}