package com.ikbalci;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

public class VoltDBClient {
    public static void main(String[] args) {
        Client client = null;
        try {
            ClientConfig clientConfig = new ClientConfig();
            client = ClientFactory.createClient(clientConfig);

            System.out.println("Attempting to connect to VoltDB server at localhost:32983");
            client.createConnection("localhost:32983");
            System.out.println("Connection successful");

            System.out.println("Calling procedure to select records from the SUBSCRIBER table");
            ClientResponse response = client.callProcedure("selectAllSubscribers");
            System.out.println("Procedure call completed");

            if (response.getStatus() == ClientResponse.SUCCESS) {
                VoltTable resultTable = response.getResults()[0];
                System.out.println("Schema of the result table:");
                for (int i = 0; i < resultTable.getColumnCount(); i++) {
                    System.out.println("Column " + i + ": " + resultTable.getColumnName(i) + " (" + resultTable.getColumnType(i) + ")");
                }

                while (resultTable.advanceRow()) {
                    System.out.println("SUBSC_ID: " + resultTable.getLong("SUBSC_ID"));
                    System.out.println("SUBSC_NAME: " + resultTable.getString("SUBSC_NAME"));
                    System.out.println("SUBSC_SURNAME: " + resultTable.getString("SUBSC_SURNAME"));
                    System.out.println("MSISDN: " + resultTable.getString("MSISDN"));
                    System.out.println("TARIFF_ID: " + resultTable.getLong("TARIFF_ID"));
                    System.out.println("START_DATE: " + resultTable.getTimestampAsTimestamp("START_DATE"));
                    System.out.println("-------------------------------");
                }
            } else {
                System.err.println("Error: " + response.getStatusString());
            }
        } catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    System.out.println("Closing the connection");
                    client.close();
                } catch (InterruptedException e) {
                    System.err.println("Failed to close client connection: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}