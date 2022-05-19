import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 */
public class Controller {
    public enum FileState{
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    public static final Logger logger = Logger.getLogger(Controller.class.toString());
    private Client currentClient = null;
    private final int cport;
    private final int r;
    private final int timeout;
    private final int rebalanced_period;
    private AtomicBoolean rebalancing = new AtomicBoolean(false);
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
    private Object storeAckLock = new Object();
    private Object removeAckLock = new Object();
    private Object generalLock = new Object();


    /**
     * @param cport Controller Port
     * @param r R factor
     * @param timeout Timeout period
     * @param rebalanced_period Rebalance period
     */
    public Controller(int cport, int r, int timeout, int rebalanced_period) {
        this.cport = cport;
        this.r = r;
        this.timeout = timeout;
        this.rebalanced_period = rebalanced_period;


        //Check Duplication
        for(Handler h: logger.getHandlers()){
            h.close();
        }

        //Set Log Path
        try{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            String logPath = "log" + File.pathSeparator + sdf.format(new Date()) + ".log";
            FileHandler fileHandler = new FileHandler(logPath,true);
            logger.addHandler(fileHandler);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Method to start the controller
     */
    public void startController() {
        try {
            System.out.println("DStore server starting...");

            ServerSocket serverSocket = new ServerSocket(cport);
            System.out.println("Waiting for Connection");

            new RebalanceThread(rebalanced_period).start();

            for (;;) {
                try {
                    Socket client = serverSocket.accept();
                    logger.info("Client: " + serverSocket.getInetAddress().getLocalHost() + "-" + client.getPort() + " has connected to DS server.");
                    new SocketThread(client, r).start();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to delete a port from valid loading port
     * @param port The port to be deleted
     */
    private synchronized void deletePortFromValidLoad(Integer port) {
        for(String fileName : validLoadPorts.keySet()) {
            if (validLoadPorts.get(fileName).contains(port)) {
                validLoadPorts.get(fileName).remove(port);
            }
        }
        System.out.println("[Controller] Port " + port + " deleted from valid load port list!");
    }

    /**
     * Choose a port to be load from a list
     * @param fileName File to be loaded
     * @return the chosen port
     */
    private synchronized Integer chooseLoadPort(String fileName) {
        var port = loadChoosePort.get(fileName).get(0);
        loadChoosePort.get(fileName).remove(0);
        return port;
    }


    /**
     * Update Store Factor to balance the files for dstore to store
     */
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
     * @return a list of ports hold the file
     */
    private synchronized List<Integer> chooseStorePorts(Integer r_factor) {
        ArrayList<Integer> dStores = new ArrayList<>();
        storeFactor.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(dStoreDoubleEntry -> {
            dStores.add(dStoreDoubleEntry.getKey().getPort());
        });

        return dStores.subList(0,r_factor);
    }

    /**
     * Choose DStores according to store factor(RF/N) sorted from small to large.
     * @param fileName File to be chosen
     * @param r_factor
     * @return a list of ports hold the file
     */
    private synchronized List<Integer> chooseStorePorts(String fileName, Integer r_factor) {
        ArrayList<Integer> dStores = new ArrayList<>();
        storeFactor.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(dStoreDoubleEntry -> {
            if(!validLoadPorts.get(fileName).contains(dStoreDoubleEntry.getKey().getPort())) {
                dStores.add(dStoreDoubleEntry.getKey().getPort());
            }
        });

        return dStores.subList(0,r_factor);
    }

    /**
     * Method to start rebalance operation
     */
    public synchronized void rebalanceOperation() {

        //DStores not enough
        if(dStoreConnections.keySet().size() < this.r) {
            logger.info("[System Warning] Dstore not enough, rebalance stop!");
            return;
        }

        synchronized (generalLock) {
            rebalancing.set(true);
            while (ackRemove.size() != 0 || ackReceive.size()!=0) {
                continue;
            }
        }

        logger.info("[System Info - Rebalance] Starting Rebalance");

        //Send LIST To DStores
        listACK.clear();

        for(Integer iport : dStoreConnections.keySet()) {
            dStoreConnections.get(iport).sendDStoreMsg(Protocol.LIST_TOKEN);
        }

        //Waiting for LIST
        logger.info("[System Info - Rebalance] Waiting for List");
        var startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() <= startTime + timeout) {// checks if file to store has completed acknowledgements
            if (listACK.size() >= dStoreConnections.size()) {
                System.out.println("[System Info - Rebalance] Confirmed LIST from all");
                break;
            }
        }
        logger.info("[System Info - Rebalance] Lists received");

        for(String fileName : validLoadPorts.keySet()) {
            if(validLoadPorts.get(fileName).size() < this.r) {
                rebalanceAdd.put(fileName, new AtomicInteger(this.r - validLoadPorts.get(fileName).size()));
            }
        }

        //Send REBALANCE To DStores
        sendRebalance();
        startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() <= startTime + timeout) {
            if (ackRebalance.get() >= dStoreConnections.keySet().size()) { // checks if file to store has completed acknowledgements
                logger.info("[System Info - Rebalance] Rebalance Successful");
                break;
            }
        }

        ackRebalance.set(0);
    }

    /**
     * Method to send rebalance command to each dstore
     */
    private synchronized void sendRebalance() {

        for (Integer port : dStoreConnections.keySet()) { // function for sorting the REBALANCE files_to_send files_to_remove
            String files_to_send = "";
            String files_to_remove = "";
            Integer files_to_send_count = 0;
            Integer files_to_remove_count = 0;

            //Remove files not in list
            for(String fileName : dStoreFiles.get(port)) {
                if(fileSizeIndex.keySet().contains(fileName)) {
                    files_to_remove += " " + fileName;
                    files_to_remove_count ++;
                    dStoreFiles.get(port).remove(fileName);
                    dStoreNumbFiles.get(port).decrementAndGet();
                }
            }

            // > RF/N send to other port
            int numbFile = dStoreNumbFiles.get(port).get();
            Double rfn = (Double.valueOf(this.r) * Double.valueOf(fileSizeIndex.keySet().size())) / (Double.valueOf(dStoreConnections.keySet().size()));

            if(numbFile > Math.ceil(rfn))  {
               int remove = (int) (numbFile - Math.ceil(rfn));
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

            dStoreConnections.get(port).sendDStoreMsg(Protocol.REBALANCE_TOKEN + " " + files_to_send_count + files_to_send + " " + files_to_remove_count + files_to_remove);
        }
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
    }


    /**
     * A Dstore instance
     */
     public class DStore {
         Integer port;
         Socket socket;

         public DStore(Integer port, Socket socket) {
             this.port = port;
             this.socket = socket;
         }

        /**
         * Method to send message to a Dstore
         * @param msg message to be sent
         */
         public void sendDStoreMsg(String msg) {
             try {
                 var dstoreWrite = new PrintWriter(socket.getOutputStream(), true);
                 dstoreWrite.println(msg);
                 logger.info("[" + cport + " -> " + port + "] " + msg);
             } catch (IOException e) {
                 logger.info("Lost Connection: " + port);
                 dStoreConnections.remove(this.port);
                 e.printStackTrace();
             }
         }

        /**
         * @return the port of the Dstore
         */
         public Integer getPort() {
             return this.port;
         }
     }

    /**
     * Thread to control Rebalance operation
     */
     class RebalanceThread extends Thread {
        private Integer rebalancePeriod;

        public RebalanceThread(Integer rebalance_period) {
            this.rebalancePeriod = rebalance_period;
        }

         @Override
         public void run() {
            while(true) {
                var startTime = System.currentTimeMillis();
                while(System.currentTimeMillis() <= startTime + rebalancePeriod) {
                    continue;
                }
                rebalanceOperation();
                rebalancing.set(false);
            }
         }
     }

    /**
     * Thread to keep connection with each client
     */
     class SocketThread extends Thread {
        Socket client;
        Integer r;

        public SocketThread (Socket socket, Integer r) {
            this.client = socket;
            this.r = r;
        }

         @Override
         public void run() {

             boolean isDStore = false;
             Integer currentDStorePort = -1;
             try {
                 BufferedReader clinetRead = new BufferedReader(new InputStreamReader(client.getInputStream()));
                 PrintWriter clientWrite = new PrintWriter(client.getOutputStream(), true);
                 String clientInput = null;

                 for(;;) {
                     clientInput = clinetRead.readLine();

                     if (clientInput != null) {

                         //Deal with input commands
                         String[] commands = clientInput.split(" ");
                         String commandToken;
                         if (commands.length == 1) {
                             commandToken = clientInput.trim();
                             commands[0] = commandToken;
                         } else {
                             commandToken = commands[0];
                         }
                         logger.info("[" + client.getPort() + " -> " + cport + " ]" + commands);

//                         System.out.println("Msg From " + currentDStorePort + ": "+ commandToken);

                         //Recognize DStore COMMAND: JOIN
                         if (commandToken.equals(Protocol.JOIN_TOKEN)) {

                             //Check duplicate dstore port
                             currentDStorePort = Integer.parseInt(commands[1]);
                             while (rebalancing.get()) {
                                 continue;
                             }

                             if (dStoreConnections.get(currentDStorePort) != null) {
                                 logger.info("[" + client.getPort() + " -> " + cport + " ] Denied! DStore port conflicts!");
                                 client.close();
                                 break;
                             }

                             isDStore = true;
                             synchronized (generalLock) {
                                 dStoreConnections.put(currentDStorePort, new DStore(currentDStorePort, client));
                             }
                             countDStore.incrementAndGet();
                             currentDStorePort = Integer.parseInt(commands[1]);
                             logger.info("Binding DStore port: " + commands[1] + " with socket\n" +
                                     "Current connected DStore: " + countDStore.get() + "/" + this.r);

                             clientWrite.println(Protocol.JOIN_SUCCESS_TOKEN);

                             if (countDStore.get() >= this.r) {
                                 logger.info("R DStores are connected, ready for client!");
                                 dStoreReady.set(true);
                             }
                             continue;

                         }

                         //Operations with Client (DStore are totally connected)
                         if (!isDStore && dStoreReady.get()) {
                             //----------Client----------

                             //COMMAND: LIST
                             if (commandToken.equals(Protocol.LIST_TOKEN)) {

                                 if (commands.length != 1) {
                                     logger.info("[System Warning] Wrong LIST COMMAND");
                                     continue;
                                 }

                                 if (fileSizeIndex.keySet().size() == 0) {
                                     clientWrite.println(Protocol.LIST_TOKEN);
                                     logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.LIST_TOKEN);
                                 } else {
                                     String file_list = "";
                                     for (String i : fileSizeIndex.keySet()) {
                                         file_list = file_list + " " + i;
                                     }
                                     clientWrite.println(Protocol.LIST_TOKEN + file_list);
                                     logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.LIST_TOKEN + file_list);
                                 }

                             }

                             //COMMAND: STORE
                             else if (commandToken.equals(Protocol.STORE_TOKEN)) {

                                 //Check length of STORE command from Client
                                 if (commands.length != 3) {
                                     logger.info("[System Warning] Wrong STORE COMMAND");
                                     continue;
                                 }

                                 String fileName = commands[1];
                                 Integer fileSize = Integer.parseInt(commands[2]);

                                 synchronized (generalLock) {
                                     //If fileName duplicates
                                     if (fileStateIndex.containsKey(fileName)) {
                                         clientWrite.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                         logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                         continue;
                                     }

                                     //If Rebalance is working
                                     while (rebalancing.get()) {
                                         continue;
                                     }
                                 }

                                 //Update file lists
                                 fileStateIndex.put(fileName, FileState.STORE_IN_PROGRESS);
                                 ackReceive.put(fileName, new AtomicInteger(0));
                                 validLoadPorts.put(fileName, new ArrayList<Integer>());

                                 //Send Msg to client
                                 logger.info("[Controller] START TO RECEIVE ACK: " + fileName);
                                 updateStoreFactor();
                                 String msg = "";
                                 for (Integer i : chooseStorePorts(this.r)) {
                                     msg += " " + i;
                                 }
                                 clientWrite.println(Protocol.STORE_TO_TOKEN + msg);
                                 System.out.println("[" + cport + " -> " + client.getPort() + "] " + Protocol.STORE_TO_TOKEN + msg);

                                 boolean storeComplete = false;
                                 var startTime = System.currentTimeMillis();

                                 //Timeout Setting
                                 while (System.currentTimeMillis() <= startTime + timeout) {
                                     if (ackReceive.get(fileName).get() >= r) {
                                         clientWrite.println(Protocol.STORE_COMPLETE_TOKEN);
                                         logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.STORE_COMPLETE_TOKEN);
                                         fileStateIndex.remove(fileName);
                                         fileStateIndex.put(fileName, FileState.STORE_COMPLETE);
                                         fileSizeIndex.put(fileName, fileSize);
                                         updateStoreFactor();
                                         storeComplete = true;
                                         break;
                                     }
                                 }

                                 if (!storeComplete) {
                                     logger.info("[System Warning] " + fileName + " Store timeout");
                                     validLoadPorts.remove(fileName);
                                     fileStateIndex.remove(fileName);
                                 }

                                 synchronized (storeAckLock) {
                                     ackReceive.remove(fileName);
                                 }
                             }

                             //COMMAND: LOAD && RELOAD
                             else if (commandToken.equals(Protocol.LOAD_TOKEN) || commands[0].equals(Protocol.RELOAD_TOKEN)) {
                                 String fileName;
                                 //Check length of LOAD command from Client
                                 if (commands.length != 2) {
                                     logger.info("[System Warning] Wrong LOAD && RELOAD COMMAND");
                                     continue;
                                 }

                                 //Check file exists
                                 fileName = commands[1];
                                 if (fileSizeIndex.keySet().contains(fileName)) {

                                     //Wait for rebalance
                                     while (rebalancing.get()) {
                                         continue;
                                     }

                                     //Response to Client: LOAD_FROM port filesize
                                     if (commands[0].equals(Protocol.LOAD_TOKEN)) {
                                         //Remove last load operation from the list
                                         if (loadChoosePort.containsKey(fileName)) {
                                             loadChoosePort.remove(fileName);
                                         }

                                         loadChoosePort.put(fileName, validLoadPorts.get(fileName));
                                         clientWrite.println(Protocol.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                         logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                     }

                                     // RELOAD
                                     else {
                                         if (loadChoosePort.get(fileName).size() == 0) {
                                             //Cannot connect to any port
                                             clientWrite.println(Protocol.ERROR_LOAD_TOKEN);
                                             logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.ERROR_LOAD_TOKEN);
                                         } else {
                                             clientWrite.println(Protocol.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                             logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.LOAD_FROM_TOKEN + " " + chooseLoadPort(fileName) + " " + fileSizeIndex.get(fileName));
                                         }
                                     }

                                 } else {
                                     clientWrite.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                     logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                 }

                             }

                             //COMMAND: REMOVE
                             else if (commandToken.equals(Protocol.REMOVE_TOKEN)) {
                                 if (commands.length != 2) {
                                     System.err.println("[System Warning] Wrong REMOVE COMMAND");
                                     continue;
                                 }

                                 String fileName = commands[1];

                                 if (!fileSizeIndex.containsKey(fileName)) {
                                     clientWrite.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                     logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                     continue;
                                 }

                                 //Wait for rebalance
                                 while (rebalancing.get()) {
                                     continue;
                                 }

                                 synchronized (generalLock) {
                                     //Update Index
                                     if(fileStateIndex.keySet().contains(fileName)) {
                                         fileStateIndex.remove(fileName);
                                     }
                                     fileStateIndex.put(fileName, FileState.REMOVE_IN_PROGRESS);
                                 }

                                 //Send REMOVE Command
                                 for (DStore i : dStoreConnections.values()) {
                                     i.sendDStoreMsg(Protocol.REMOVE_TOKEN + " " + fileName);
                                     logger.info("[" + cport + " -> " + i.getPort() + "] " + Protocol.REMOVE_TOKEN + " " + fileName);
                                 }

                                 synchronized (removeAckLock) {
                                     ackRemove.put(fileName, new AtomicInteger());
                                 }

                                 boolean removeComplete = false;

                                 //Timeout Setting
                                 var startTime = System.currentTimeMillis();
                                 while (System.currentTimeMillis() <= startTime + timeout) {
                                     if (ackRemove.get(fileName).get() == r) {
                                         removeComplete = true;
                                         clientWrite.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                         fileSizeIndex.remove(fileName);
                                         break;
                                     }
                                 }

                                 if (!removeComplete) {
                                     logger.info("[Controller] REMOVE timeout. File: "+ fileName);
                                 }

                                 ackRemove.remove(fileName);
                                 fileStateIndex.remove(fileName);
                                 validLoadPorts.remove(fileName);
                             }

                         }

                         //Operations with Client (DStore aren't totally connected)
                         else if (!isDStore && !dStoreReady.get()) {
                             clientWrite.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                             logger.info("[" + cport + " -> " + client.getPort() + "] " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                         }

                         //Operations with DStore
                         else if (isDStore) {

                             //COMMAND: STORE_ACK filename
                             if (commandToken.equals(Protocol.STORE_ACK_TOKEN)) {
                                 if (commands.length != 2) {
                                     logger.info("[System Warning] Wrong STORE_ACK Command");
                                 }
                                 String fileName = commands[1].trim();

                                 //Update File Index State when receive ack
                                 synchronized (storeAckLock) {
                                     if (ackReceive.keySet().contains(fileName)) {
                                         ackReceive.get(fileName).incrementAndGet();
                                         validLoadPorts.get(fileName).add(currentDStorePort);
                                     } else {
                                         logger.info("[Controller] ACK file not exists" + ackReceive.keySet());
                                     }
                                 }
                             }

                             //COMMAND: REMOVE_ACK filename
                             else if (commandToken.equals(Protocol.REMOVE_ACK_TOKEN)) {
                                 if (commands.length != 2) {
                                     logger.info("[System Warning] Wrong STORE_ACK Command");
                                 }
                                 String fileName = commands[1].trim();

                                 //Update File Index State when receive ack
                                 synchronized (removeAckLock) {
                                     if (ackRemove.containsKey(fileName)) {
                                         ackRemove.get(fileName).incrementAndGet();
                                         if (validLoadPorts.get(fileName) != null) {
                                             validLoadPorts.get(fileName).remove(currentDStorePort);
                                         }
                                     } else {
                                         logger.info("[Controller] ACK file not exists");
                                     }
                                 }
                             }

                             //COMMAND: REBALANCE_COMPLETE
                             else if (commandToken.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                                 logger.info("DStore port:" + currentDStorePort + " REBALANCE COMPELETE!");
                                 ackRebalance.incrementAndGet();
                             }

                             //COMMAND: LIST
                             else if (commandToken.equals(Protocol.LIST_TOKEN)) {
                                 ArrayList<String> fileList = new ArrayList<>(Arrays.asList(commands));
                                 fileList.remove(0);

                                 dStoreNumbFiles.put(currentDStorePort, new AtomicInteger(fileList.size()));
                                 dStoreFiles.put(currentDStorePort, fileList);

                                 //Update Valid Load Port
                                 validLoadPorts.clear();
                                 for (String fileName : fileList) {
                                     if (validLoadPorts.get(fileName) == null) {
                                         validLoadPorts.put(fileName, new ArrayList<Integer>());
                                     }
                                     if (!validLoadPorts.get(fileName).contains(currentDStorePort)) {
                                         validLoadPorts.get(fileName).add(currentDStorePort);
                                     }
                                 }
                                 listACK.add(currentDStorePort);
                             }
                         }

                     } else {
                         if (isDStore) {
                             logger.info("[System Error] DStore Disconnected! Port: "+ currentDStorePort);
                             dStoreConnections.remove(currentDStorePort);
                             deletePortFromValidLoad(currentDStorePort);
                             countDStore.decrementAndGet();
                             if (countDStore.get() < this.r) {
                                 dStoreReady.set(false);
                             }
                             logger.info("[Controller] Current connected DStore: " + countDStore.get() + "/" + this.r);
                         } else {
                             logger.info("[System Error] Client Disconnected!");
                         }
                         client.close();
                         break;
                     }
                 }

             } catch (Exception e) {
                 if (isDStore) {
                     logger.info("[System Error] DStore Disconnected! Port: "+ currentDStorePort);
                     logger.info("[Controller] Current connected DStore: " + countDStore.get() + "/" + this.r);
                     dStoreConnections.remove(currentDStorePort);
                     countDStore.decrementAndGet();
                     if (countDStore.get() < this.r) {
                         dStoreReady.set(false);
                     }
                 }
                 logger.info(e.toString());
                 return;
             }
         }
     }
    

}
