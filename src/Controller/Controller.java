import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Controller {
    private enum FileState{
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }
    public class Client {
        Integer port;
        Socket socket;
        PrintWriter clientWrite;

        public Client(Integer port, Socket socket) {
            this.port = port;
            this.socket = socket;
        }
        
        public void sendClientMsg(String msg) {
            try {
                PrintWriter clientWrite = new PrintWriter(socket.getOutputStream(), true);
                clientWrite.println(msg);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    private Client currentClient = null;
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalanced_period; 
    private AtomicInteger countDStore = new AtomicInteger(0); // count number of connected dstores
    private AtomicBoolean dStoreReady = new AtomicBoolean(false);
    private ConcurrentHashMap<String, ArrayList<Integer>> validLoadPorts = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, FileState> fileStateIndex = new ConcurrentHashMap<>(); //Index of file with state
    private ConcurrentHashMap<String, Integer> fileSizeIndex = new ConcurrentHashMap<>(); //Index of stored file with filesize
    private ConcurrentHashMap<Integer, Socket> dStoreConnections = new ConcurrentHashMap<>(); //Bind client socket with dstore port
    private ConcurrentHashMap<String, Integer> ackReceive = new ConcurrentHashMap<>(); //Count received acks for each file
    private ConcurrentHashMap<String, Integer> ackRemove = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, Integer> dStoreLoad = new ConcurrentHashMap<>();

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
                    boolean isDStore = false;
                    Integer currentDStorePort = null;
                    try {
                        BufferedReader clinetRead = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        PrintWriter clientWrite = new PrintWriter(client.getOutputStream(), true);
                        String clientInput = null;
                        // client.setSoTimeout(timeout);

                        while(true) {
                            clientInput = clinetRead.readLine();

                            if (clientInput != null) {
                                System.out.println(clientInput);

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

                                } else {
                                    //Set client connection state
                                    currentClient = new Client(client.getPort(), client);
                                }

                                //Operations with Client (DStore are totally connected)
                                if (!isDStore && dStoreReady.get()) {

                                    //----------Client----------
                                    //COMMAND: LIST
                                    if (commands[0].equals(Protocal.LIST_TOKEN)) {

                                        if(commands.length != 1) {
                                            System.err.println("Wrong List COMMAND");
                                            continue;
                                        }

                                        if (fileSizeIndex.keySet().size() == 0) {
                                            clientWrite.println(Protocal.LIST_TOKEN);
                                        } 

                                        else {
                                            String file_list = "";
                                            showFileStateIndex();
                                            for(String i : fileSizeIndex.keySet()) {
                                                file_list = file_list + " " + i;
                                            }
                                            clientWrite.println(Protocal.LIST_TOKEN + " " + file_list);
                                        }
                                    }

                                    //COMMAND: STORE
                                    else if (commands[0].equals(Protocal.STORE_TOKEN)) {
                                        //Check length of STORE command from Client
                                        if (commands.length != 3) {
                                            //TODO: Add Log
                                            System.err.println("Wrong STORE command");
                                            continue;
                                        } 
                                        //If DStores are enough
                                        else if (!dStoreReady.get()) {
                                            clientWrite.println(Protocal.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                                            continue;
                                        }
                                        
                                        String fileName = commands[1];
                                        Integer fileSize = Integer.parseInt(commands[2]);

                                        //If fileName duplicates
                                        if (fileStateIndex.containsKey(fileName)) {
                                            clientWrite.println(Protocal.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                            continue;
                                        }
                                        
                                        fileStateIndex.put(fileName, FileState.STORE_IN_PROGRESS);
                                        ackReceive.put(fileName, 0);
                                        clientWrite.println(Protocal.STORE_TO_TOKEN + " " + listExistPort());
                                        System.out.print(Protocal.STORE_TO_TOKEN + " " + listExistPort());
                                        boolean storeComplete = false;

                                        //Timeout Setting
                                        while(System.currentTimeMillis() <= System.currentTimeMillis() + timeout) {
                                            if(ackReceive.get(fileName) >= r) {
                                                clientWrite.println(Protocal.STORE_COMPLETE_TOKEN);
                                                fileStateIndex.remove(fileName);
                                                fileStateIndex.put(fileName, FileState.STORE_COMPLETE);
                                                fileSizeIndex.put(fileName, fileSize);
                                                storeComplete = true;
                                                break;
                                            }
                                        }

                                        if (!storeComplete) {
                                            System.out.println(fileName + " Store timeout");
                                        }
                    
                                        ackReceive.remove(fileName);
                                    }
                                    
                                    //COMMAND: LOAD && RELOAD
                                    else if (commands[0].equals(Protocal.LOAD_TOKEN) || commands[0].equals(Protocal.RELOAD_TOKEN)) {
                                        String fileName;
                                        //Check length of LOAD command from Client
                                        if (commands.length != 2) {
                                            System.err.println("Unknow commands: " + commands);
                                            continue;
                                        }

                                        //Check file exists
                                        fileName = commands[1];
                                        if (fileStateIndex.containsKey(fileName) && fileStateIndex.get(fileName) == FileState.STORE_COMPLETE) {
                                            clientWrite.println(Protocal.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                        } else {
                                            clientWrite.println(Protocal.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                            continue;
                                        }

                                        //Response to Client: LOAD_FROM port filesize
                                        if (commands[0].equals(Protocal.LOAD_TOKEN)) {
                                            //Remove last load operation from the list
                                            if (validLoadPorts.contains(fileName)) {
                                                validLoadPorts.remove(fileName);
                                            }

                                            ArrayList<Integer> portArray = new ArrayList<>();
                                                for(Integer i : dStoreConnections.keySet()) {
                                                    portArray.add(i);
                                                }
                                            validLoadPorts.put(fileName, portArray);
                                            clientWrite.println(Protocal.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                        } else {
                                            validLoadPorts.get(fileName).remove(0);
                                            if(validLoadPorts.get(fileName) == null) {
                                                //Cannot connect to any port
                                                clientWrite.println(Protocal.ERROR_LOAD_TOKEN);
                                            } else {
                                                clientWrite.println(Protocal.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                            }
                                        }

                                    }

                                    //COMMAND: REMOVE
                                    else if (commands[0].equals(Protocal.REMOVE_TOKEN)) {
                                        if (commands.length != 2) {
                                            System.err.println("Wrong REMOVE command");
                                            continue;
                                        }
                                        
                                        String fileName = commands[1];
                                        if (!fileSizeIndex.contains(fileName)) {
                                            clientWrite.println(Protocal.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                            continue;
                                        } 
                                        
                                        //Update Index
                                        fileSizeIndex.remove(fileName);
                                        fileStateIndex.remove(fileName);
                                        fileStateIndex.put(fileName, FileState.REMOVE_IN_PROGRESS);

                                        for(Socket i : dStoreConnections.values()) {
                                            PrintWriter sendMsg = new PrintWriter(i.getOutputStream(), true);
                                            sendMsg.println(Protocal.REMOVE_TOKEN);
                                        }

                                        ackRemove.put(fileName, 0);
                                        boolean removeComplete = false;
                                        while(System.currentTimeMillis() <= System.currentTimeMillis() + timeout ) {
                                            if (ackRemove.get(fileName) == r) {
                                                fileStateIndex.remove(fileName);
                                                fileStateIndex.put(fileName, FileState.REMOVE_COMPLETE);
                                                removeComplete = true;
                                                clientWrite.println(Protocal.REMOVE_COMPLETE_TOKEN);
                                                break;
                                            }
                                        }
                                        
                                        if (!removeComplete) {
                                            System.err.println("REMOVE timeout. File: " + fileName);
                                        }
                                        ackRemove.remove(fileName);
                                    }

                                } 
                                
                                //Operations with Client (DStore aren't totally connected)
                                else if(!isDStore && !dStoreReady.get()) {
                                    clientWrite.println(Protocal.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                                }

                                //Operations with DStore
                                else {
                                    //COMMAND: STORE_ACK filename
                                    if(commands[0].equals(Protocal.STORE_ACK_TOKEN)) {
                                        if(commands.length != 2) {
                                            System.err.println("Wrong STORE_ACK command format");
                                        }
                                        String fileName = commands[1].trim();

                                        //Update File Index State when receive ack
                                        if(ackReceive.containsKey(fileName)) {
                                            Integer currentValue = ackReceive.get(fileName);
                                            ackReceive.remove(fileName);
                                            ackReceive.put(fileName, currentValue + 1);
                                        } else {
                                            System.err.println("ACK file not exists");
                                        }
                                    }

                                }
                            } else {
                                if (isDStore) {
                                    System.out.println("ERROR: DStore Disconnected!");
                                    dStoreConnections.remove(currentDStorePort);
                                    countDStore.decrementAndGet();
                                    dStoreReady.set(false);
                                    System.out.println("Current connected DStore: " + countDStore.get() + "/" + this.r);
                                } else {
                                    System.out.println("ERROR: Client Disconnected!");
                                }
                                client.close();
                                break;
                            }
                        }   

                    } catch (Exception e) {
                        if (isDStore) {
                            System.out.println("ERROR: DStore Disconnected!");
                            dStoreConnections.remove(currentDStorePort);
                            countDStore.decrementAndGet();
                            dStoreReady.set(false);
                        }
                        e.printStackTrace();
                        System.out.println("Current connected DStore: " + countDStore.get() + "/" + this.r);
                        return;
                    }
                }).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkDStoreEnough() {

    }

    private String listExistPort() {
        String list = "";
        for (Integer port : dStoreConnections.keySet())
        {
            list += Integer.toString(port) + " ";
        }
        return list;
    }

    private Integer chooseLoadPort(String fileName) {
        var port = validLoadPorts.get(fileName).get(0);
        return port;
    }

    private void showFileStateIndex() {
        System.out.print(fileStateIndex.toString());
    }

    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),  Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.startController();
    }
}
