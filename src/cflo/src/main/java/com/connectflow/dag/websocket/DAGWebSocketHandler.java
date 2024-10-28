package com.connectflow.dag.websocket;

import com.connectflow.dag.listeners.DAGWebSocketListener;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


@WebSocket
public class DAGWebSocketHandler {

    private static Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private static DAGWebSocketListener<?, ?> dagWebSocketListener;

    public static void setDagWebSocketListener(DAGWebSocketListener<?, ?> listener) {
        dagWebSocketListener = listener;
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        sessions.add(session);
        System.out.println("WebSocket Connected: " + session);
        if (dagWebSocketListener != null) {
            dagWebSocketListener.sendInitialGraph(session);
        }
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        sessions.remove(session);
        System.out.println("WebSocket Closed: " + session);
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) {
    }

    public static void broadcastMessage(String message) {
        sessions.forEach(session -> {
            try {
                if (session.isOpen()) {
                    session.getRemote().sendString(message);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static void sendMessage(Session session, String message) {
        try {
            if (session.isOpen()) {
                session.getRemote().sendString(message);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
