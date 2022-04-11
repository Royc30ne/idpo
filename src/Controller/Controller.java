package Controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;


public class Controller {
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalanced_period;

    public Controller(int cport, int r, int timeout, int rebalanced_period) {
        this.cport = cport;
        this.r = r;
        this.timeout = timeout;
        this.rebalanced_period = rebalanced_period;
    }

    public void startController() {
        try {
            System.out.println("DStore server starting...");
            ServerSocket serverSocket = new ServerSocket(cport);
            System.out.println("Waiting for Connection");

            while(true) {
                Socket client = serverSocket.accept();
                System.out.print("Client: " + serverSocket.getInetAddress().getLocalHost() + " has connected to DS server.");
                
                BufferedReader bf = new BufferedReader(new InputStreamReader(client.getInputStream()));

                client.close();
                break;
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),  Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.startController();
    }
}
