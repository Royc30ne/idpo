import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;


public class Controller {
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalanced_period; 
    private ConcurrentHashMap<String, String> file_index; 

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
                
                new Thread(() -> {
                    try {
                        BufferedReader clinetRead = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        PrintWriter clientWrite = new PrintWriter(client.getOutputStream(), true);

                        String clientInput = null;

                        while(true) {
                            clientInput = clinetRead.readLine().trim();
                            System.out.println(clientInput);

                            if (clientInput != null){
                                String[] commands = clientInput.split(" ");

                                if (commands[0].equals(Protocal.LIST_TOKEN)) {
                                    clientWrite.println(clientList());
                                }
                            }
                        }

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }).start();
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private String clientList() {
        return Protocal.LIST_TOKEN + " testfile";
    }

    private void clientStore() {

    }

    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),  Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.startController();
    }
}
