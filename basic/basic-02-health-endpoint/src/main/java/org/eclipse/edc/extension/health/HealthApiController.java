/*
 *  Copyright (c) 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - Initial implementation
 *
 */

package org.eclipse.edc.extension.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@Path("/")
public class HealthApiController {

    private final Monitor monitor;
    private final Connection connection;
    private final ObjectMapper objectMapper;

    public HealthApiController(Monitor monitor) {
        this.monitor = monitor;
        Connection conn = null;
        try {
            conn = DatabaseUtil.getConnection();
        } catch (SQLException e) {
            monitor.info("Failed to establish connection: " + e.getMessage());
        }
        this.connection = conn;
        this.objectMapper = new ObjectMapper();
    }

    @GET
    @Path("health")
    public String checkHealth() {
        monitor.info("Received a health request");
        return "{\"response\":\"I'm alive!\"}";
    }

    @POST
    @Path("saveUsers")
    public String saveUsers(String message) {
        monitor.info("Received a saveUsers request");
        try {
            if (connection != null) {
                JsonNode usersNode = objectMapper.readTree(message);

                for (JsonNode userNode : usersNode) {
                    String sql = "INSERT INTO users (id, name, username, email, street, suite, city, zipcode, lat, lng, phone, website, company_name, company_catchPhrase, company_bs) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET " +
                            "name = EXCLUDED.name, " +
                            "username = EXCLUDED.username, " +
                            "email = EXCLUDED.email, " +
                            "street = EXCLUDED.street, " +
                            "suite = EXCLUDED.suite, " +
                            "city = EXCLUDED.city, " +
                            "zipcode = EXCLUDED.zipcode, " +
                            "lat = EXCLUDED.lat, " +
                            "lng = EXCLUDED.lng, " +
                            "phone = EXCLUDED.phone, " +
                            "website = EXCLUDED.website, " +
                            "company_name = EXCLUDED.company_name, " +
                            "company_catchPhrase = EXCLUDED.company_catchPhrase, " +
                            "company_bs = EXCLUDED.company_bs";

                    PreparedStatement stmt = connection.prepareStatement(sql);
                    stmt.setInt(1, userNode.get("id").asInt());
                    stmt.setString(2, userNode.get("name").asText());
                    stmt.setString(3, userNode.get("username").asText());
                    stmt.setString(4, userNode.get("email").asText());
                    stmt.setString(5, userNode.get("address").get("street").asText());
                    stmt.setString(6, userNode.get("address").get("suite").asText());
                    stmt.setString(7, userNode.get("address").get("city").asText());
                    stmt.setString(8, userNode.get("address").get("zipcode").asText());
                    stmt.setString(9, userNode.get("address").get("geo").get("lat").asText());
                    stmt.setString(10, userNode.get("address").get("geo").get("lng").asText());
                    stmt.setString(11, userNode.get("phone").asText());
                    stmt.setString(12, userNode.get("website").asText());
                    stmt.setString(13, userNode.get("company").get("name").asText());
                    stmt.setString(14, userNode.get("company").get("catchPhrase").asText());
                    stmt.setString(15, userNode.get("company").get("bs").asText());
                    stmt.executeUpdate();
                }
                return "{\"response\":\"Messages saved successfully!\"}";
            } else {
                return "{\"error\":\"No database connection available.\"}";
            }
        } catch (SQLException e) {
            monitor.info("Error saving message to database: " + e.getMessage());
            return "{\"error\":\"Failed to save message.\"}";
        } catch (IOException e) {
            monitor.info("IO Error: " + e.getMessage());
            return "{\"error\":\"Failed to save message.\"}";
        }
    }

    @GET
    @Path("getUsers")
    public String getUsers() {
        monitor.info("Received a getUsers request.");

        if (connection == null) {
            monitor.info("No database connection available.");
            return "{\"error\":\"No database connection available.\"}";
        }

        try {
            
            String sql = "SELECT id, name, username, email, street, suite, city, zipcode, lat, lng, phone, website, company_name, company_catchPhrase, company_bs FROM users";
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            ArrayNode usersArray = objectMapper.createArrayNode();

            while (rs.next()) {
                ObjectNode userNode = objectMapper.createObjectNode();
                userNode.put("id", rs.getInt("id"));
                userNode.put("name", rs.getString("name"));
                userNode.put("username", rs.getString("username"));
                userNode.put("email", rs.getString("email"));

                ObjectNode addressNode = objectMapper.createObjectNode();
                addressNode.put("street", rs.getString("street"));
                addressNode.put("suite", rs.getString("suite"));
                addressNode.put("city", rs.getString("city"));
                addressNode.put("zipcode", rs.getString("zipcode"));

                ObjectNode geoNode = objectMapper.createObjectNode();
                geoNode.put("lat", rs.getString("lat"));
                geoNode.put("lng", rs.getString("lng"));
                addressNode.set("geo", geoNode);

                userNode.set("address", addressNode);

                userNode.put("phone", rs.getString("phone"));
                userNode.put("website", rs.getString("website"));

                ObjectNode companyNode = objectMapper.createObjectNode();
                companyNode.put("name", rs.getString("company_name"));
                companyNode.put("catchPhrase", rs.getString("company_catchPhrase"));
                companyNode.put("bs", rs.getString("company_bs"));

                userNode.set("company", companyNode);

                usersArray.add(userNode);
            }

            return usersArray.toString();

        } catch (SQLException e) {
            monitor.info("Error retrieving users from database: " + e.getMessage());
            return "{\"error\":\"Failed to retrieve users.\"}";
        }
    }
}
