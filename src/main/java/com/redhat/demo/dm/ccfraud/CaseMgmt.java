package com.redhat.demo.dm.ccfraud;



import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFact;

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
                    "/services/rest/server/containers/FraudCaseMgmtSolution_1.0.0/processes/com.myspace.fraudcasemgmtsolution.FraudCaseWorkflow/instances");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization","Basic YWRtaW5Vc2VyOlJlZEhhdA==");

            OutputStream os = conn.getOutputStream();
            os.write(new Gson().toJson(potentialFraudFact).getBytes());
            os.flush();

            if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode()+conn.getResponseMessage());
            }

            conn.disconnect();

        } catch (Exception e) {

            e.printStackTrace();

        }

    }



}
