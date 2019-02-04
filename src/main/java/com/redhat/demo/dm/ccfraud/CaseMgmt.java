package com.redhat.demo.dm.ccfraud;



import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFact;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFactCaseFile;

import java.io.IOException;
import java.io.OutputStream;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;

public class CaseMgmt {


    public void invokeCase(PotentialFraudFact potentialFraudFact) {

        try {

            URL url = new URL("http://myapp-kieserver.case-mgmt.svc:8080" +
                    "/services/rest/server/containers/test-case-project_1.0.0/processes/src.fraudWorkflow/instances");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization","Basic YWRtaW5Vc2VyOlJlZEhhdA==");

            PotentialFraudFactCaseFile potentialFraudFactCaseFile = new PotentialFraudFactCaseFile(String.valueOf(potentialFraudFact.getCreditCardNumber()),potentialFraudFact.getTransactions().toString());



            OutputStream os = conn.getOutputStream();

            os.write(new Gson().toJson(potentialFraudFactCaseFile).getBytes());
            os.flush();



            if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            conn.disconnect();

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }




}
