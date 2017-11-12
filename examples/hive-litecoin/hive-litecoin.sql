-- create database
create database blockchains;

use blockchains;

-- create external table representing Litecoin Blockchain data stored in /user/input/namecoin
create external table LitecoinBlockchain ROW FORMAT SERDE 'org.zuinnote.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/input/litecoin' TBLPROPERTIES("hadoopcryptoledger.bitcoinblockinputformat.filter.magic"="FBC0B6DB");

-- count the number of blocks
select count(*) from LitecoinBlockchain;

-- count the number of transactions
select count(*) from LitecoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions;