-- create database
create database blockchains;

use blockchains;

-- create table representing Bitcoin Blockchain data stored in /user/cloudera/bitcoin/input
Create table BitcoinBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapreduce.lib.output.NullOutputFormat' LOCATION '/user/cloudera/bitcoin/input';

-- The following example counts the number of blocks:

select count(*) from BitcoinBlockchain;

-- The following example counts the number of transactions:
select count(*) from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions;

-- The following example calculates the total output of all Bitcoin transactions
select SUM(expout.value) FROM (select * from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode(exptran.listofoutputs) exploded_outputs as expout;

