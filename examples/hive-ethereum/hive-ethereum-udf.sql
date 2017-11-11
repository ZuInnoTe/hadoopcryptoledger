-- demonstrate the capabilities of the HadoopCryptoLedger Hive UDF for Ethereum 

-- assumption is that you have deployed the UDFs: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hive-UDF
-- and created in the database 'blockchains' the table 'EthereumBlockChain' using the HiveSerde (see also https://github.com/ZuInnoTe/hadoopcryptoledger/blob/master/examples/hive-ethereum/hive-ethereum.sql)
-- and made the functions as temporary functions available. if you use permanent functions you need to add the prefix hcl.





---the following example shows the from (sendAddress) of a transaction
SELECT hclEthereumGetSendAddress(ethereumTransactions[0]) FROM EthereumBlockchain LIMIT 1;

---the following example shows the from (sendAddress) of a transaction and convert it to a hex string to search in popular Ethereum block explorers
SELECT hex(hclEthereumGetSendAddress(ethereumTransactions[0])) FROM EthereumBlockchain LIMIT 1;

---the following example shows the hash of a transaction
SELECT hclEthereumGetTransactionHash(ethereumTransactions[0]) FROM EthereumBlockchain LIMIT 1;

---the following example shows the hash of a transaction and convert it to a hex string to search in popular Ethereum block explorers
SELECT hex(hclEthereumGetTransactionHash(ethereumTransactions[0])) FROM EthereumBlockchain LIMIT 1;

--- the following example displays the chainId (if supported by chain) 
SELECT hclEthereumGetChainId(ethereumTransactions[0]) FROM EthereumBlockchain LIMIT 1;
