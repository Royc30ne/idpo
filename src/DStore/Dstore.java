import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Dstore {
    private int port;
    private final int cport;
    private final int timeout;
    private String file_folder;
    private String filePath;
    private boolean controllerConnected = false;
    private ConcurrentHashMap<String, Integer> fileList = new ConcurrentHashMap<>();
    public static final Logger logger = Logger.getLogger(Logger.class.toString());

    /**
     * @param port Dstore port
     * @param cport Controller port
     * @param timeout Timeout period
     * @param file_folder Folder Name
     */
    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;

        logger.setLevel(Level.ALL);

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
     * Method to start controller
     */
    public void start() {
        File folder = new File(file_folder);
        if (!folder.exists()) {
            logger.info("Creating Folder: " + file_folder);
            System.out.println();
            folder.mkdir();
        } else {
            logger.info("Folder exists");
        }
        
        //Initialise DStore folder
        filePath = folder.getAbsolutePath();
        clearFolder(folder);

        try {
            logger.info("DStore Port: " + port);
            Socket controller = new Socket(InetAddress.getByName("localhost"), cport);
            logger.info("Controller Connected.\nPort: " + cport);

            new ControllerThread(controller).start();

            logger.info("Ready for Clinet Thread");
            ServerSocket serverSocket = new ServerSocket(port);
            
            while(true) {
                logger.info("Waiting for Client to Join!");
                Socket client = serverSocket.accept();
                new ClientThread(client, controller).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to clear a folder
     * @param folder The folder to be cleared
     */
    public void clearFolder(File folder) {
        try{
            logger.info("Clearing Folder: " + folder.getName());
            for (File file : folder.listFiles()) {
                file.delete();
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    /**
     * Thread to process Controller
     */
    class ControllerThread extends Thread {
        private Socket controller;

        public ControllerThread(Socket controller) {
            this.controller = controller;
        }

        @Override
        public void run() {
            //Controller Operations
            try {
                BufferedReader readMsg = new BufferedReader(new InputStreamReader(controller.getInputStream()));
                PrintWriter sendMsg = new PrintWriter(controller.getOutputStream(), true);
            
                String readline = null;
                
                //Join Controller
                sendMsg.println(Protocol.JOIN_TOKEN + " " + port);
                logger.info("[" + port + " -> " + controller.getPort() + "] " + Protocol.JOIN_TOKEN + " " + port);

                //Process Operations
                while(true) {
                        readline = readMsg.readLine();
                        if(readline != null) {
                            logger.info("[From Controller] " + controller.getPort() + ": " + readline);
                            String[] commands = readline.split(" ");
                            var command = commands[0].trim();

                            //COMMAND: JOIN
                            if(command.equals(Protocol.JOIN_SUCCESS_TOKEN)) {
                                controllerConnected = true;
                                logger.info("Successfully build connection with Controller");
                            }
                            
                            //COMMAND: REMOVE
                            else if (command.equals(Protocol.REMOVE_TOKEN)) {
                                if(commands.length != 2) {
                                    logger.info("Wrong REMOVE command");
                                    continue;
                                }
                                
                                String fileName = commands[1];
                                File file = new File(filePath + File.separator + fileName);
                                if(!file.exists() || !file.isFile()) {
                                    logger.info("File not exists");
                                    sendMsg.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                    logger.info("[" + port + " -> " + controller.getPort() + "] " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                } 
                                
                                if(file.delete()) {
                                    sendMsg.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                    logger.info("[" + port + " -> " + controller.getPort() + "] " + Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                    if(fileList.containsKey(fileName)) {
                                        fileList.remove(fileName);
                                    }
                                }
                            }

                            //COMMAND: REBALANCE: LIST
                            else if (command.equals(Protocol.LIST_TOKEN)) {
                                if(commands.length != 1) {
                                    logger.info("Wrong LIST command");
                                    continue;
                                }

                                String list = "";
                                if (fileList.size() != 0) {
                                    for (String fileName : fileList.keySet()) {
                                        list += " " + fileName;
                                    }
                                }

                                sendMsg.println(Protocol.LIST_TOKEN + list);
                                logger.info("[" + port + " -> " + controller.getPort() + "] " + Protocol.LIST_TOKEN + list);
                            }

                            //REBALANCE
                            else if (command.equals(Protocol.REBALANCE_TOKEN)) {
                                Integer filesToSend = Integer.parseInt(commands[1]);
                                Integer index = 2;

                                for (int i = 0; i < filesToSend; i++) {
                                    String filename = commands[index];
                                    Integer portSendCount = Integer.parseInt(commands[index + 1]);

                                    for (int j = index + 2; j <= index + 1 + portSendCount; j++) {

                                        Socket dStoreSocket = new Socket(InetAddress.getByName("localhost"),Integer.parseInt(commands[j]));
                                        BufferedReader inDstore = new BufferedReader(new InputStreamReader(dStoreSocket.getInputStream()));
                                        PrintWriter outDstore = new PrintWriter(dStoreSocket.getOutputStream(), true);
                                        File existingFile = new File(filePath + File.separator + filename);
                                        Integer filesize = (int) existingFile.length(); // casting long to int file size limited to fat32
                                        outDstore.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + filesize);

                                        if (inDstore.readLine() == Protocol.ACK_TOKEN) {
                                            FileInputStream inf = new FileInputStream(existingFile);
                                            OutputStream out = dStoreSocket.getOutputStream();
                                            out.write(inf.readNBytes(filesize));
                                            out.flush();
                                            inf.close();
                                            out.close();
                                            dStoreSocket.close();
                                        } else {
                                            dStoreSocket.close();
                                        }
                                    }
                                    index = index + portSendCount + 2; // ready index for next file
                                }

                                Integer fileRemoveCount = Integer.parseInt(commands[index]);
                                for (int z = index + 1; z < index + 1 + fileRemoveCount; z++) {
                                    File existingFile = new File(filePath + File.separator + commands[z]);
                                    if (existingFile.exists()) {
                                        existingFile.delete();
                                    }
                                }


                                sendMsg.println(Protocol.REBALANCE_COMPLETE_TOKEN);
                                logger.info("[" + port + " -> " + controller.getPort() + "] " + Protocol.REBALANCE_COMPLETE_TOKEN);
                            }

                            else {
                                logger.info("Unknown command!");
                                continue;
                            }
                        }

                } 
            } catch (Exception e1) {
                try {
                    e1.printStackTrace();
                    controller.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                controllerConnected = false;
            }
        }

    }

    /**
     * Thread to process Client
     */
    class ClientThread extends Thread {
        private Socket client;
        private Socket controller;
        public ClientThread(Socket client, Socket controller) {
            this.client = client;
            this.controller =controller;
        }

        @Override
        public void run() {
            try {
                PrintWriter sendController = new PrintWriter(controller.getOutputStream(), true);
                PrintWriter sendClinet = new PrintWriter(client.getOutputStream(), true);
                BufferedReader receiveClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
                InputStream writeStream = client.getInputStream();
                String commandLine;
                logger.info("Client Connected");

                while(true) {
                    commandLine = receiveClient.readLine();
                    if(commandLine != null) {
                        String[] commands = commandLine.split(" ");
                        String command = commands[0].trim();
                        System.out.println("[From Client]" + client.getPort() + ": " + commands);
                        
                        //COMMAND: STORE
                        if(command.equals(Protocol.STORE_TOKEN) || command.equals(Protocol.REBALANCE_STORE_TOKEN)) {
                            //Receive STORE command from Client
                            if(commands.length != 3) {
                                logger.info("Wrong STORE command");
                                continue;
                            } else {
                                sendClinet.println(Protocol.ACK_TOKEN);
                            }

                            //Receive file stream from Client
                            String fileName = commands[1].trim();
                            Integer fileSize = Integer.parseInt(commands[2]);
                            File file = new File(filePath + File.separator + fileName);
                            FileOutputStream fos = new FileOutputStream(file);

                            var startTime = System.currentTimeMillis();
                            while(System.currentTimeMillis() <= startTime + timeout) {
                                fos.write(writeStream.readNBytes(fileSize));
                                if (command.equals(Protocol.STORE_TOKEN)) {
                                    sendController.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
                                    logger.info("[" + port + " -> " + cport + "] " + Protocol.STORE_ACK_TOKEN + " " + fileName);
                                }
                                fileList.put(fileName, fileSize);
                                break;
                            }

                            //Done. Close all connections
                            fos.flush();
                            fos.close();
                            client.close();
                            return;
                        } 
                        
                        //COMMAND: LOAD_DATA
                        else if(command.equals(Protocol.LOAD_DATA_TOKEN)) {
                            if(commands.length != 2) {
                                logger.info("Wrong LOAD_DATA command");
                                continue;
                            } 
                            
                            String fileName = commands[1];
                            File file = new File(filePath + File.separator + fileName);

                            if(!file.exists() || !file.isFile()) {
                                logger.info("Load File Not Exists");
                                client.close();
                                return;
                            }

                            FileInputStream fileStream = new FileInputStream(file);
                            OutputStream sendStream = client.getOutputStream();
                            var dos = new DataOutputStream(sendStream);

                            dos.writeUTF(file.getName());
                            dos.flush();
                            dos.writeLong(file.length());
                            dos.flush();

                            //Start Sending
                            byte[] bytes = new byte[1024];
                            int length = 0;
                            long progress = 0;
                            while((length = fileStream.read(bytes, 0, bytes.length)) != -1) {
                                dos.write(bytes, 0, length);
                                dos.flush();
                                progress += length;
                                logger.info("| " + (100*progress/file.length()) + "% |");
                            }

                            //Done. Close all connections
                            dos.flush();
                            dos.close();
                            fileStream.close();
                            sendStream.close();
                            client.close();
                            return;
                        } 

                        else {
                            logger.info("Unknown Command");
                            continue;
                        }
                    }
                }

            } catch (Exception e) {
                try {
                    e.printStackTrace();
                    client.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } 
        }
        
    }

    public static void main(String[] args) {
        Dstore dStore = new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), String.valueOf(args[3]));
        dStore.start();
    }

}
