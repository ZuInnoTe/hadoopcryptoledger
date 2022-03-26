package org.zuinnote.hadoop.ethereum.format.common;

import org.zuinnote.hadoop.ethereum.format.common.rlp.RLPElement;

import java.math.BigInteger;

public interface EthereumTransactionInterface
{
    public static BigInteger convertVarNumberToBigInteger(byte[] rawData) {
        BigInteger result=BigInteger.ZERO;
        if (rawData!=null) {
            if (rawData.length>0) {
                result = new BigInteger(1,rawData); // we know it is always positive
            }
        }
        return result;
    }

    public static BigInteger convertVarNumberToBigInteger(RLPElement rpe) {

        return convertVarNumberToBigInteger(rpe.getRawData());
    }
}
