package org.zuinnote.hadoop.bitcoin.format;

public class BitcoinTransactionElement {
    private byte[] blockHash;
    private int transactionIdxInBlock;
    private byte[] transactionHash;
    private int type;
    private long indexInTransaction;
    private long amount;
    private byte[] script;

    public byte[] getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(byte[] blockHash) {
        this.blockHash = blockHash;
    }

    public int getTransactionIdxInBlock() {
        return transactionIdxInBlock;
    }

    public void setTransactionIdxInBlock(int transactionIdxInBlock) {
        this.transactionIdxInBlock = transactionIdxInBlock;
    }

    public byte[] getTransactionHash() {
        return transactionHash;
    }

    public void setTransactionHash(byte[] transactionHash) {
        this.transactionHash = transactionHash;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getIndexInTransaction() {
        return indexInTransaction;
    }

    public void setIndexInTransaction(long indexInTransaction) {
        this.indexInTransaction = indexInTransaction;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }


    public byte[] getScript() {
        return script;
    }

    public void setScript(byte[] script) {
        this.script = script;
    }
}
