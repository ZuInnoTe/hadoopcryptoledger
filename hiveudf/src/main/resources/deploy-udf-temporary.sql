CREATE TEMPORARY FUNCTION hclBitcoinTransactionHash as 'org.zuinnote.hadoop.bitcoin.hive.udf.BitcoinTransactionHashUDF' USING JAR '/tmp/hadoopcryptoledger-hiveudf-1.0.7.jar';
CREATE TEMPORARY FUNCTION hclBitcoinTransactionHashSegwit as 'org.zuinnote.hadoop.bitcoin.hive.udf.BitcoinTransactionHashSegwitUDF' USING JAR '/tmp/hadoopcryptoledger-hiveudf-1.0.7.jar';
CREATE TEMPORARY FUNCTION hclBitcoinScriptPattern as 'org.zuinnote.hadoop.bitcoin.hive.udf.BitcoinScriptPaymentPatternAnalyzerUDF' USING JAR '/tmp/hadoopcryptoledger-hiveudf-1.0.7.jar';
