import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Controller {
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalanced_period; 
    private AtomicInteger countDStore = new AtomicInteger(0); // count number of connected dstores
    private AtomicBoolean dStoreReady = new AtomicBoolean(false);
    private ConcurrentHashMap<Integer, Socket> dStoreConnections = new ConcurrentHashMap<>(); //Bind client socket with dstore port

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
                System.out.print("Client: " + serverSocket.getInetAddress().getLocalHost() + " has connected to DS server. \n");
                
                new Thread(() -> {
                    try {
                        BufferedReader clinetRead = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        PrintWriter clientWrite = new PrintWriter(client.getOutputStream(), true);
                        boolean isDStore = false;
                        String clientInput = null;
                        Integer currentDStorePort = null;

                        while(true) {
                            clientInput = clinetRead.readLine();
                            System.out.println(clientInput);

                            if (clientInput != null && !dStoreReady.get()){
                                String[] commands = clientInput.split(" ");

                                //Recognize DStore
                                if (commands[0].equals(Protocal.JOIN_TOKEN)) {
                                    //Check duplicate dstore port
                                    currentDStorePort = Integer.parseInt(commands[1]);

                                    if (dStoreConnections.get(currentDStorePort) != null) {
                                        System.out.println("DStore port already exist!");
                                        client.close();
                                        break;
                                    }
                                    
                                    //
                                    isDStore = true;
                                    dStoreConnections.put(currentDStorePort, client);
                                    countDStore.incrementAndGet();
                                    currentDStorePort = Integer.parseInt(commands[1]);
                                    System.out.println("Binding DStore port: " + commands[1] + " with socket\nCurrent connected DStore: " + countDStore.get() + "/" + this.r);

                                    if (countDStore.get() == this.r) {
                                        System.out.println("All DStores are connected, ready for client!");
                                        dStoreReady.set(true);
                                    }
                                    clientWrite.println(Protocal.JOIN_SUCCESS_TOKEN);
                                    
                                    continue;
                                }

                                //Operations with Client (DStore are totally connected)
                                if(!isDStore && dStoreReady.get()) {

                                    //----------Client----------
                                    //COMMAND: LIST
                                    if (commands[0].equals(Protocal.LIST_TOKEN)) {
                                        clientWrite.println(clientList());
                                    }

                                } 
                                
                                //Operations with Client (DStore aren't totally connected)
                                else if(!isDStore && !dStoreReady.get()) {
                                        
                                }

                                //Operations with DStore
                                else {
                                    

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
