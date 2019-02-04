package com.redhat.demo.dm.ccfraud.domain;

public class PotentialFraudFactCaseFile {

    private String caseFile_creditCardNumber;
    private String caseFile_transactions;
    private String caseFile_transactionList;


    public String getCaseFile_transactionList() {
        return caseFile_transactionList;
    }

    public void setCaseFile_transactionList(String caseFile_transactionList) {
        this.caseFile_transactionList = caseFile_transactionList;
    }

    public String getCaseFile_creditCardNumber() {
        return caseFile_creditCardNumber;
    }

    public void setCaseFile_creditCardNumber(String caseFile_creditCardNumber) {
        this.caseFile_creditCardNumber = caseFile_creditCardNumber;
    }

    public String getCaseFile_transactions() {
        return caseFile_transactions;
    }

    public void setCaseFile_transactions(String caseFile_transactions) {
        this.caseFile_transactions = caseFile_transactions;
    }

    public PotentialFraudFactCaseFile(String caseFile_creditCardNumber, String caseFile_transactions) {
        this.caseFile_creditCardNumber = caseFile_creditCardNumber;
        this.caseFile_transactions = caseFile_transactions;
    }

    @Override
    public String toString() {
        return "PotentialFraudFactCaseFile{" +
                "caseFile_creditCardNumber='" + caseFile_creditCardNumber + '\'' +
                ", caseFile_transactions='" + caseFile_transactions + '\'' +
                '}';
    }
}
