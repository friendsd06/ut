package com.connectflow.dag;

import static spark.Spark.*;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class SparkWebExample {
    private static List<User> users = new ArrayList<>();
    private static Gson gson = new Gson();

    public static void main(String[] args) {
        try {
            // Configure Spark
            port(4567);
            threadPool(8);

            // Increase timeouts
            before((request, response) -> {
                response.header("Connection", "keep-alive");
                response.header("Keep-Alive", "timeout=60");
            });

            // CORS configuration
            options("/*", (request, response) -> {
                response.header("Access-Control-Allow-Origin", "*");
                response.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
                response.header("Access-Control-Allow-Headers", "*");
                response.header("Access-Control-Max-Age", "3600");
                return "OK";
            });

            before((request, response) -> {
                response.header("Access-Control-Allow-Origin", "*");
                response.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
                response.header("Access-Control-Allow-Headers", "*");
                response.type("application/json");
            });

            // Initialize sample data
            users.add(new User(1, "John Doe", "john@example.com"));
            users.add(new User(2, "Jane Smith", "jane@example.com"));

            // Basic health check endpoint
            get("/health", (req, res) -> {
                Map<String, String> health = new HashMap<>();
                health.put("status", "UP");
                return gson.toJson(health);
            });

            // Root endpoint
            get("/", (req, res) -> {
                Map<String, String> response = new HashMap<>();
                response.put("message", "Welcome to Spark Web API");
                response.put("status", "running");
                return gson.toJson(response);
            });

            // Get all users
            get("/users", (req, res) -> gson.toJson(users));

            // Get user by ID
            get("/users/:id", (req, res) -> {
                int id = Integer.parseInt(req.params(":id"));
                User user = users.stream()
                        .filter(u -> u.getId() == id)
                        .findFirst()
                        .orElse(null);

                if (user == null) {
                    res.status(404);
                    return gson.toJson(Map.of("error", "User not found"));
                }
                return gson.toJson(user);
            });

            // Add server started message
            awaitInitialization();
            System.out.println("Server started at http://localhost:4567");

        } catch (Exception e) {
            System.err.println("Server failed to start: " + e.getMessage());
            e.printStackTrace();
            stop();
        }
    }
}

class User {
    private int id;
    private String name;
    private String email;

    public User() {}

    public User(int id, String name, String email) {
        this.id = id;
        this.name = name;
        this.email = email;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}