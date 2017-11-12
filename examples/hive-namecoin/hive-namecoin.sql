-- create database
create database blockchains;

use blockchains;

-- create external table representing Namecoin Blockchain data stored in /user/input/namecoin
create external table NamecoinBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/input/namecoin' TBLPROPERTIES("hadoopcryptoledger.bitcoinblockinputformat.filter.magic"="F9BEB4FE","hadoopcryptoledger.bitcoinblockinputformat.readauxpow"="true");

-- count the number of blocks
select count(*) from NamecoinBlockchain;

-- count the number of transactions
select count(*) from NamecoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions;