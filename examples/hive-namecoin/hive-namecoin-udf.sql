-- demonstrate the capabilities of the HadoopCryptoLedger Hive UDF for Bitcoin 

-- assumption is that you have deployed the UDFs: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hive-UDF
-- and created in the database 'blockchains' the table 'NamecoinBlockChain' using the HiveSerde (see also https://github.com/ZuInnoTe/hadoopcryptoledger/blob/master/examples/hive-namecoin/hive-namecoin.sql)
-- and made the functions as temporary functions available. if you use permanent functions you need to add the prefix hcl.

-- extract type of namecoin information (e.g. new, firstupdate, update)
SELECT hclNamecoinGetNameOperation(expout.txoutscript) FROM (select * from NamecoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode (exptran.listofoutputs) exploded_outputs as expout;


-- extracts information from the firstupdate and update operation (e.g. domain name, domain information etc.)
SELECT hclNamecoinExtractField(expout.txoutscript) FROM (select * from NamecoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode (exptran.listofoutputs) exploded_outputs as expout;
