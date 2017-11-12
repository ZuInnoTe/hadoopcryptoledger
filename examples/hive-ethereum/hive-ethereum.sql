-- create database
create database blockchains;

use blockchains;

-- create external table representing Ethereum Blockchain data stored in /user/input/ethereum
create external table EthereumBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.ethereum.hive.serde.EthereumBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.ethereum.format.mapred.EthereumBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/input/ethereum';

-- count the number of blocks
select count(*) from EthereumBlockchain;

-- count the number of transactions
select count(*) from EthereumBlockchain LATERAL VIEW explode(ethereumTransactions) exploded_transactions;

-- summarize the value of all transactions
select SUM(exptran.value) FROM (select * from EthereumBlockchain LATERAL VIEW explode(ethereumTransactions) exploded_transactions as exptran) ttable;  