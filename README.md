# Hadoop Crypto Ledger (hadoopcryptoledger)
This repository will provide various components to read crypto ledgers, such as the Bitcoin blockchain, with Hadoop and Ecosystem components. More particularly, the current plans are to provide:
* a set of FileInputFormats for Hadoop to read crypto ledgers, such as the Bitcoin blockchain, with Hadoop applications.
 * Bitcoin
  * BitcoinBlockInputformat: Deserializes blocks containing transactions into Java-Object(s). Each record is an object of the class BitcoinBlock containing Transactions (class BitcoinTransaction). Best suitable if you want to have flexible analytics. The key (ie unique identifier) of the block is currently a byte array containing hashMerkleRoot (32 Byte).
  * BitcoinRawBlockInputformat: Each record is a byte array containing the raw bitcoin block data. he key (ie unique identifier) of the block is currently a byte array containing hashMerkleRoot (32 Byte). This is most suitable if you are only interested in a small part of the data and do not want to waste time on deserialization.
  * BitcoinTransactionInputFormat: Deserializes Bitcoin transactions into Java-Object(s). Each record is an object of class BitcoinTransaction. Transactions are identifiable by an byte array containg the block hashMerkleRoot (32 Byte) and transactionCounter (9 byte). They do not contain block header data. The granularity is much higher than in the BitcoinBlockInputFormat. This make sense if you anyway want to analyse each transaction independently (e.g. if you want to do some analytics on the scripts within a transaction and combine the results later on). 
* a set Hive Serdes to read transactions from crypto ledgers, such as the Bitcoin blockchain, with Hive by representing them as normal tables. These tables can then be joined with other tables containing other relevant information, such as stock market movements or weather patterns. The following Hive Serde are available
 * Bitcoin
  * BitcoinBlockHiveSerde: Represents information about BitcoinBlock(s) in a table. The identifier is hashMerkleRoot. Does NOT include the transactions.
  * BitcoinTransactionHiveSerde: Represents information about transactions. Can be linked to a BitcoinBlock table via block hashMerkleRoot
  * Hint: It is recommended to create a flat (sorted) table in ORC format (compressed Snappy or Zlib) by joining two tables created using the aforementioned Serdes. This enables fast interactive analytics on Hive.

If you want to test it:
* Bitcoin: Use Bitcoin core in the most recent version and download as well as verify the whole Bitcoin blockchain (Attention: currently around 70 GB (March 2016) . Afterwards, copy ~/.bitcoin/blocks/*.dat to a folder on HDFS, e.g. /data/blockchain
 * Test file input format (tbd) - It is recommended to use it with TEZ, but you can use it also in other frameworks, such as MapReduce
 * Test Hive serde (tbd)
