/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/
package org.zuinnote.flink.ethereum.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import org.zuinnote.flink.ethereum._

import  org.zuinnote.hadoop.ethereum.format.mapreduce.AbstractEthereumRecordReader

/**
*
*/

object FlinkScalaEthereumBlockCounter {
   def main(args: Array[String]): Unit = {
        val  env = ExecutionEnvironment.getExecutionEnvironment
        val params: ParameterTool = ParameterTool.fromArgs(args)
        countTotalTransactions(env,params.get("input"),params.get("output"))
        env.execute("Flink Scala Ethereum transaction counter")
      }


      def countTotalTransactions(env: ExecutionEnvironment, inputFile: String, outputFile: String): Unit = {
        val inputFormat = new EthereumBlockFlinkInputFormat(AbstractEthereumRecordReader.DEFAULT_MAXSIZE_ETHEREUMBLOCK,false)
        val blockChainData = env.readFile(inputFormat, inputFile)
        val totalTransactionCounts = blockChainData.map{ethereumBlock => ("Number of Transactions: ",ethereumBlock.getEthereumTransactions().size())}
          .groupBy(0)
          .sum(1)
 
       totalTransactionCounts.writeAsText(outputFile,FileSystem.WriteMode.OVERWRITE)
     }

 }


