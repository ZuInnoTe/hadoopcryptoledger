-- create database
create database blockchains;

use blockchains;

-- create external table representing Bitcoin Blockchain data stored in /user/cloudera/bitcoin/input
create external table BitcoinBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/cloudera/bitcoin/input' TBLPROPERTIES("hadoopcryptoledger.bitcoinblockinputformat.filter.magic"="F9BEB4D9");

-- create external table representing Bitcoin Blockchain data from Testnet3
create external table BitcoinBlockchainTestNet3 ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/cloudera/bitcoin/input' TBLPROPERTIES("hadoopcryptoledger.bitcoinblockinputformat.filter.magic"="0B110907");

-- The following example counts the number of blocks:

select count(*) from BitcoinBlockchain;

-- The following example counts the number of transactions:
select count(*) from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions;

-- The following example calculates the total output of all Bitcoin transactions
select SUM(expout.value) FROM (select * from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode(exptran.listofoutputs) exploded_outputs as expout;

-- The following example gets the number of transaction outputs to the Bitocoin address C825A1ECF2A6830C4401620C3A16F1995057C2AB (please keep in mind that this is the raw address used in Bitcoin, find out how to convert it from a user friendly here: https://en.bitcoin.it/wiki/List_of_address_prefixes)
-- please note that this covers only transaction using the Pay to hash mechanism: https://en.bitcoin.it/wiki/Pay_to_script_hash
select count(*) FROM (select * from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode(exptran.listofoutputs) exploded_outputs as expout WHERE instr(substring(regexp_extract(hex(expout.txoutscript),"76A914.*88AC",0),7,41),"C825A1ECF2A6830C4401620C3A16F1995057C2AB")>0;

