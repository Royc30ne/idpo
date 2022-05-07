import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Controller {
    public enum FileState{
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }
    
    private Client currentClient = null;
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalanced_period; 
    private AtomicInteger countDStore = new AtomicInteger(0); // count number of connected dstores
    private AtomicBoolean dStoreReady = new AtomicBoolean(false);
    private ConcurrentHashMap<String, ArrayList<Integer>> validLoadPorts = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ArrayList<Integer>> loadChoosePort = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, FileState> fileStateIndex = new ConcurrentHashMap<>(); //Index of file with state
    private ConcurrentHashMap<String, Integer> fileSizeIndex = new ConcurrentHashMap<>(); //Index of stored file with filesize
    private ConcurrentHashMap<Integer, DStore> dStoreConnections = new ConcurrentHashMap<>(); //Bind client socket with dstore port
    private ConcurrentHashMap<DStore, Double> storeFactor = new ConcurrentHashMap<>(); //Sort priority of store
    private ConcurrentHashMap<String, AtomicInteger> ackReceive = new ConcurrentHashMap<>(); //Count received acks for each file
    private ConcurrentHashMap<String, AtomicInteger> ackRemove = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, Integer> dStoreLoad = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, AtomicInteger> dStoreNumbFiles = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, ArrayList<String>> dStoreFiles = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, AtomicInteger> rebalanceAdd = new ConcurrentHashMap<>();
    private AtomicInteger ackRebalance = new AtomicInteger(0);
    private List<Integer> listACK = Collections.synchronizedList(new ArrayList<Integer>());


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
                                        System.out.println("DStore port in used!");
                                        client.close();
                                        break;
                                    }
                                    
                                    isDStore = true;
                                    dStoreConnections.put(currentDStorePort, new DStore(currentDStorePort, client));
                                    countDStore.incrementAndGet();
                                    currentDStorePort = Integer.parseInt(commands[1]);
                                    System.out.println("Binding DStore port: " + commands[1] + " with socket\n" +
                                            "Current connected DStore: " + countDStore.get() + "/" + this.r);

                                    if (countDStore.get() == this.r) {
                                        System.out.println("All DStores are connected, ready for client!");
                                        dStoreReady.set(true);
                                    }
                                    clientWrite.println(Protocal.JOIN_SUCCESS_TOKEN);
//                                    rebalanceOperation();
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
                                            clientWrite.println(Protocal.LIST_TOKEN + file_list);
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

                                        //Update file lists
                                        fileStateIndex.put(fileName, FileState.STORE_IN_PROGRESS);
                                        ackReceive.put(fileName, new AtomicInteger(0));
                                        validLoadPorts.put(fileName, new ArrayList<Integer>());

                                        System.out.println("LOCAL: STARTING RECEIVE ACK FILENAME: " + fileName);
                                        updateStoreFactor();
                                        String msg = "";
                                        for(Integer i : chooseStorePorts(this.r)) {
                                            msg += " " + i;
                                        }
                                        clientWrite.println(Protocal.STORE_TO_TOKEN + msg);
                                        System.out.println("SEND TO CLIENT: " + Protocal.STORE_TO_TOKEN + msg);
                                        boolean storeComplete = false;

                                        //Timeout Setting
                                        while(System.currentTimeMillis() <= System.currentTimeMillis() + timeout) {
                                            if(ackReceive.get(fileName).get() >= r) {
                                                clientWrite.println(Protocal.STORE_COMPLETE_TOKEN);
                                                System.out.println("SEND TO CLIENT-" + client.getPort() + ": STORE COMPLETE");
                                                fileStateIndex.remove(fileName);
                                                fileStateIndex.put(fileName, FileState.STORE_COMPLETE);
                                                fileSizeIndex.put(fileName, fileSize);
                                                updateStoreFactor();
                                                storeComplete = true;
                                                break;
                                            }
                                        }

                                        if (!storeComplete) {
                                            System.out.println(fileName + " Store timeout");
                                            validLoadPorts.remove(fileName);
                                            fileStateIndex.remove(fileName);
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

                                            //Response to Client: LOAD_FROM port filesize
                                            if (commands[0].equals(Protocal.LOAD_TOKEN)) {
                                                //Remove last load operation from the list
                                            if (loadChoosePort.containsKey(fileName)) {
                                                loadChoosePort.remove(fileName);
                                            }

                                            loadChoosePort.put(fileName, validLoadPorts.get(fileName));
                                            clientWrite.println(Protocal.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                            }

                                            // RELOAD
                                            else {

                                                if(loadChoosePort.get(fileName) == null) {
                                                    //Cannot connect to any port
                                                    clientWrite.println(Protocal.ERROR_LOAD_TOKEN);
                                                } else {
                                                    clientWrite.println(Protocal.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                                }
                                            }

                                        } else {
                                            clientWrite.println(Protocal.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                            continue;
                                        }

                                    }

                                    //COMMAND: REMOVE
                                    else if (commands[0].equals(Protocal.REMOVE_TOKEN)) {
                                        if (commands.length != 2) {
                                            System.err.println("Wrong REMOVE command");
                                            continue;
                                        }
                                        
                                        String fileName = commands[1];
                                        if (!fileSizeIndex.containsKey(fileName)) {
                                            clientWrite.println(Protocal.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                            continue;
                                        } 
                                        
                                        //Update Index
                                        fileStateIndex.remove(fileName);
                                        fileStateIndex.put(fileName, FileState.REMOVE_IN_PROGRESS);

                                        for(DStore i : dStoreConnections.values()) {
                                            i.sendDStoreMsg(Protocal.REMOVE_TOKEN + " " + fileName);
                                        }

                                        ackRemove.put(fileName, new AtomicInteger());
                                        boolean removeComplete = false;
                                        while(System.currentTimeMillis() <= System.currentTimeMillis() + timeout ) {
                                            if (ackRemove.get(fileName).get() == r) {
                                                removeComplete = true;
                                                clientWrite.println(Protocal.REMOVE_COMPLETE_TOKEN);
                                                validLoadPorts.remove(fileName);
                                                fileSizeIndex.remove(fileName);
                                                break;
                                            }
                                        }
                                        
                                        if (!removeComplete) {
                                            System.err.println("REMOVE timeout. File: " + fileName);
                                        }

                                        ackRemove.remove(fileName);
                                        fileStateIndex.remove(fileName);
                                        validLoadPorts.remove(fileName);
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
                                        if(ackReceive.keySet().contains(fileName)) {
                                            ackReceive.get(fileName).incrementAndGet();
                                            validLoadPorts.get(fileName).add(currentDStorePort);
                                            System.out.println(ackReceive.get(fileName).get());
                                        } else {
                                            System.err.println("ACK file not exists" + ackReceive.keySet());
                                        }
                                    }

                                    //COMMAND: REMOVE_ACK filename
                                    else if (commands[0].equals(Protocal.REMOVE_ACK_TOKEN)) {
                                        if(commands.length != 2) {
                                            System.err.println("Wrong STORE_ACK command format");
                                        }
                                        String fileName = commands[1].trim();

                                        //Update File Index State when receive ack
                                        if(ackRemove.containsKey(fileName)) {
                                            ackRemove.get(fileName).incrementAndGet();
                                            if(validLoadPorts.get(fileName) != null) {
                                                validLoadPorts.get(fileName).remove(currentDStorePort);
                                            }
                                        } else {
                                            System.err.println("ACK file not exists");
                                        }
                                    }

                                    //COMMAND: REBALANCE_COMPLETE
                                    else if(commands[0].equals(Protocal.REBALANCE_COMPLETE_TOKEN)) {
                                        ackRebalance.incrementAndGet();
                                    }

                                    //COMMAND: LIST
                                    else if (commands[0].equals(Protocal.LIST_TOKEN)) {
                                        ArrayList<String> fileList = new ArrayList<>(Arrays.asList(commands));
                                        fileList.remove(0);
                                        dStoreNumbFiles.put(currentDStorePort, new AtomicInteger(fileList.size()));
                                        dStoreFiles.put(currentDStorePort, fileList);

                                        //Update Valid Load Port
                                        validLoadPorts.clear();
                                        for (String fileName : fileList) {
                                            if(validLoadPorts.get(fileName) != null) {
                                                validLoadPorts.put(fileName, new ArrayList<Integer>());
                                            }
                                            if(!validLoadPorts.get(fileName).contains(currentDStorePort)) {
                                                validLoadPorts.get(fileName).add(currentDStorePort);
                                            }
                                        }

                                        listACK.add(currentDStorePort);
                                    }
                                }
                            } else {
                                if (isDStore) {
                                    System.out.println("ERROR: DStore Disconnected! Port: " + currentDStorePort);
                                    dStoreConnections.remove(currentDStorePort);
                                    deletePortFromValidLoad(currentDStorePort);
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

    private void deletePortFromValidLoad(Integer port) {
        for(String fileName : validLoadPorts.keySet()) {
            if (validLoadPorts.get(fileName).contains(port)) {
                validLoadPorts.get(fileName).remove(port);
            }
        }
        System.out.println("Local Info: Port " + port + " deleted from valid load port list!");
    }

    private Integer chooseLoadPort(String fileName) {
        var port = loadChoosePort.get(fileName).get(0);
        loadChoosePort.get(fileName).remove(0);
        return port;
    }

    private void showFileStateIndex() {
        System.out.print(fileStateIndex.toString());
    }

    private void updateStoreFactor() {
        storeFactor.clear();

        for(Integer i : dStoreConnections.keySet()) {
            DStore dStore = dStoreConnections.get(i);
            Integer fileCount = 0;

            for(ArrayList<Integer> iSet : validLoadPorts.values()) {
                if(iSet.contains(i)) {
                    fileCount++;
                }
            }

            storeFactor.put(dStore, (Double.valueOf(this.r) * Double.valueOf(fileCount)) / Double.valueOf(dStoreConnections.keySet().size()));
        }
        System.out.println("Current Store Factor: " + storeFactor);
    }


    /**
     * Choose DStores according to store factor(RF/N) sorted from small to large.
     * @param r_factor
     * @return
     */
    private List<Integer> chooseStorePorts(Integer r_factor) {
        ArrayList<Integer> dStores = new ArrayList<>();
        storeFactor.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(dStoreDoubleEntry -> {
            dStores.add(dStoreDoubleEntry.getKey().getPort());
        });

        return dStores.subList(0,r_factor);
    }

    private List<Integer> chooseStorePorts(String fileName, Integer r_factor) {
        ArrayList<Integer> dStores = new ArrayList<>();
        storeFactor.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(dStoreDoubleEntry -> {
            if(!validLoadPorts.get(fileName).contains(dStoreDoubleEntry.getKey().getPort())) {
                dStores.add(dStoreDoubleEntry.getKey().getPort());
            }
        });

        return dStores.subList(0,r_factor);
    }

    public synchronized void rebalanceOperation() {

        //Send LIST To DStores
        listACK.clear();

        for(DStore ds : dStoreConnections.values()) {
            ds.sendDStoreMsg(Protocal.LIST_TOKEN);
        }

        while (System.currentTimeMillis() <= System.currentTimeMillis()+timeout) {// checks if file to store has completed acknowledgements
            if (listACK.size() >= dStoreConnections.size()) {
                System.out.println("Rebalance: Confirmed LIST from all");
                break;
            }
        }

        for(String fileName : validLoadPorts.keySet()) {

            if(validLoadPorts.get(fileName).size() < this.r) {
                rebalanceAdd.put(fileName, new AtomicInteger(this.r - validLoadPorts.get(fileName).size()));
            }
        }

        //Send REBALANCE To DStores
        sendRebalance();
        while (System.currentTimeMillis() <= System.currentTimeMillis()+timeout) {
            if (ackRebalance.get() >= dStoreConnections.keySet().size()) { // checks if file to store has completed acknowledgements
                System.out.println("*********************REBALANCE SUCCESSFULL*********************");
                break;
            }
        }

        ackRebalance.set(0);
    }

    private void sendRebalance() {

        for (Integer port : dStoreConnections.keySet()) { // function for sorting the REBALANCE files_to_send files_to_remove
            String files_to_send = "";
            String files_to_remove = "";
            Integer files_to_send_count = 0;
            Integer files_to_remove_count = 0;

            //Remove files not in list
            for(String fileName : dStoreFiles.get(port)) {
                if(fileSizeIndex.get(fileName) == null) {
                    files_to_remove += " " + fileName;
                    files_to_remove_count ++;
                    dStoreFiles.get(port).remove(fileName);
                    dStoreNumbFiles.get(port).decrementAndGet();
                }
            }

            // > RF/N send to other port
            int numbFile = dStoreNumbFiles.get(dStoreConnections.get(port)).get();
            Double rfn = (Double.valueOf(this.r) * Double.valueOf(fileSizeIndex.keySet().size())) / (Double.valueOf(dStoreConnections.keySet().size()));

            if(numbFile > rfn)  {
               int remove = (int) (Math.ceil(rfn) - numbFile);
               for(String fileName : dStoreFiles.get(port).subList(0,remove)) {
                   int send_port = chooseStorePorts(fileName,1).get(0);
                   files_to_send += fileName + " 1 " + send_port;
                   files_to_remove += " " + fileName;
                   dStoreFiles.get(port).remove(fileName);
                   dStoreNumbFiles.get(port).decrementAndGet();
                   dStoreFiles.get(send_port).add(fileName);
                   dStoreNumbFiles.get(send_port).incrementAndGet();
               }
            }

            // rebalance add
            for (String fileName : rebalanceAdd.keySet()) {
                if(dStoreFiles.get(port).contains(fileName)) {
                    String ports = "";
                    List<Integer> send_ports = chooseStorePorts(fileName,rebalanceAdd.get(fileName).get());

                    for(Integer iport : send_ports){
                        ports += " " + iport;
                    }
                    files_to_send += fileName + " " + rebalanceAdd.get(fileName).get() + ports;
                    files_to_send_count ++;
                    rebalanceAdd.remove(fileName);

                    for(Integer iport : send_ports) {
                        dStoreFiles.get(iport).add(fileName);
                        dStoreNumbFiles.get(iport).incrementAndGet();
                    }
                }
            }

            dStoreConnections.get(port).sendDStoreMsg(Protocal.REBALANCE_TOKEN + " " + files_to_send_count + files_to_send + " " + files_to_remove_count + files_to_remove);
        }
    }

    public DStore chooseDStoreDoesNoHaveFile(String fileName, Integer i) {
        var validDStores = validLoadPorts.get(fileName);
        var ansList = new ArrayList<Integer>();
        dStoreConnections.keySet().stream().toList().forEach(integer -> {
            if(!validDStores.contains(integer)) {
                ansList.add(integer);
            }
        });
        return dStoreConnections.get(ansList.get(i));
    }

    public HashMap<String,Integer> checkFilesLessR() {
        HashMap<String,Integer> fileList = new HashMap<>();
        for(String fileName : validLoadPorts.keySet()) {
            if(validLoadPorts.get(fileName).size() < this.r) {
               fileList.put(fileName, this.r - validLoadPorts.get(fileName).size());
            }
        }
        return fileList;
    }

    public HashMap<String,Integer> checkFilesLargerR() {
        HashMap<String,Integer> fileList = new HashMap<>();
        for(String fileName : validLoadPorts.keySet()) {
            if(validLoadPorts.get(fileName).size() > this.r) {
                fileList.put(fileName, validLoadPorts.get(fileName).size() - this.r);
            }
        }
        return fileList;
    }


    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),  Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.startController();
    }

    public class Client {
        Integer port;
        Socket socket;

        public Client(Integer port, Socket socket) {
            this.port = port;
            this.socket = socket;
        }
        
        public void sendClientMsg(String msg) {
            try {
                var clientWrite = new PrintWriter(socket.getOutputStream(), true);
                clientWrite.println(msg);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

     public class DStore {
         Integer port;
         Socket socket;

         public DStore(Integer port, Socket socket) {
             this.port = port;
             this.socket = socket;
         }

         public void sendDStoreMsg(String msg) {
             try {
                 var dstoreWrite = new PrintWriter(socket.getOutputStream(), true);
                 dstoreWrite.println(msg);
             } catch (IOException e) {
                 dStoreConnections.remove(this.port);
                 e.printStackTrace();
             }
         }

         public Integer getPort() {
             return this.port;
         }
     }

     class RebalanceThread implements Runnable {
        private Integer rebalancePeriod;

        public RebalanceThread(Integer rebalance_period) {
            this.rebalancePeriod = rebalance_period;
        }

         @Override
         public void run() {
            while(true) {
                while(System.currentTimeMillis() <= System.currentTimeMillis() + rebalancePeriod) {
                    continue;
                }
                rebalanceOperation();
            }
         }
     }
    

}
