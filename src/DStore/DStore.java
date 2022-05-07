import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class DStore {
    private int port;
    private final int cport;
    private final int timeout;
    private String file_folder;
    private String filePath;
    private boolean controllerConnected = false;
    private ConcurrentHashMap<String, Integer> fileList = new ConcurrentHashMap<>();

    public DStore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }

    public void start() {
        File folder = new File(file_folder);
        if (!folder.exists()) {
            System.out.println("Creating Folder: " + file_folder);
            folder.mkdir();
        } else {
            System.out.println("Folder exists");
        }
        
        //Initialise DStore folder
        filePath = folder.getAbsolutePath();
        clearFolder(folder);

        try {
            System.out.println("DStore Port: " + port);
            Socket controller = new Socket(InetAddress.getByName("localhost"), cport);
            System.out.println("Controller Connected.\nPort: " + cport);
            
            // new ControllerThread(con troller).start();
            new Thread(new ControllerThread(controller)).start();

            // if (!controllerConnected) {
            //     return;
            // } 

            System.out.println("Ready for Clinet Thread");
            ServerSocket serverSocket = new ServerSocket(port);
            
            while(true) {
                System.out.println("Waiting for Client to Join!");
                Socket client = serverSocket.accept();
                new Thread(new ClientThread(client, controller)).start();
                // new ClientThread(client, controller);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void clearFolder(File folder) {
        try{
            System.out.println("Clearing Folder: " + folder.getName());
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
    class ControllerThread implements Runnable {
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
                sendMsg.println(Protocal.JOIN_TOKEN + " " + port);

                //Process Operations
                while(true) {
                        readline = readMsg.readLine().trim();
                        if(readline != null) {
                            System.out.println("Controller Thread--" + readline);
                            String[] commands = readline.split(" ");
                            var command = commands[0].trim();

                            //COMMAND: JOIN
                            if(command.equals(Protocal.JOIN_SUCCESS_TOKEN)) {
                                controllerConnected = true;
                                System.out.println("Successfully build connection with Controller");
                            }
                            
                            //COMMAND: REMOVE
                            else if (command.equals(Protocal.REMOVE_TOKEN)) {
                                if(commands.length != 2) {
                                    System.err.println("Wrong REMOVE command");
                                    continue;
                                }
                                
                                String fileName = commands[1];
                                File file = new File(filePath + File.separator + fileName);
                                if(!file.exists() || !file.isFile()) {
                                    System.err.println("File not exists");
                                    sendMsg.println(Protocal.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                } 
                                
                                if(file.delete()) {
                                    sendMsg.println(Protocal.REMOVE_ACK_TOKEN + " " + fileName);
                                    if(fileList.containsKey(fileName)) {
                                        fileList.remove(fileName);
                                    }
                                }
                            }

                            //COMMAND: REBALANCE: LIST
                            else if (command.equals(Protocal.LIST_TOKEN)) {
                                if(commands.length != 1) {
                                    System.err.println("Wrong LIST command");
                                    continue;
                                }

                                String list = "";
                                for(String fileName : fileList.keySet()) {
                                    list += " " + fileName;
                                }

                                sendMsg.println(Protocal.LIST_TOKEN + list);
                            }

                            else if (command.equals(Protocal.REBALANCE_TOKEN)) {
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
                                        outDstore.println(Protocal.REBALANCE_STORE_TOKEN + " " + filename + " " + filesize);

                                        if (inDstore.readLine() == Protocal.ACK_TOKEN) {
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

                                sendMsg.println(Protocal.REBALANCE_COMPLETE_TOKEN);
                            }

                            else {
                                System.err.println("Unknown command!");
                                continue;
                            }
                        }

                } 
            } catch (IOException e1) {
                e1.printStackTrace();
                controllerConnected = false;
            }
        }

    }

    /**
     * Thread to process Client
     */
    class ClientThread implements Runnable {
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
                System.out.println("Client connected");

                while(true) {
                    commandLine = receiveClient.readLine();
                    if(commandLine != null) {
                        String[] commands = commandLine.split(" ");
                        String command = commands[0].trim();
                        System.out.println("Client Thread--" + commandLine);
                        
                        //COMMAND: STORE
                        if(command.equals(Protocal.STORE_TOKEN) || command.equals(Protocal.REBALANCE_STORE_TOKEN)) {
                            //Receive STORE command from Client
                            if(commands.length != 3) {
                                System.err.println("Wrong STORE command");
                                continue;
                            } else {
                                sendClinet.println(Protocal.ACK_TOKEN);
                            }

                            //Receive file stream from Client
                            String fileName = commands[1].trim();
                            Integer fileSize = Integer.parseInt(commands[2]);
                            File file = new File(filePath + File.separator + fileName);
                            FileOutputStream fos = new FileOutputStream(file);
                            
                            while(System.currentTimeMillis() <= System.currentTimeMillis() + timeout) {
                                fos.write(writeStream.readNBytes(fileSize));
                                sendController.println(Protocal.STORE_ACK_TOKEN + " " + fileName);
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
                        else if(command.equals(Protocal.LOAD_DATA_TOKEN)) {
                            if(commands.length != 2) {
                                System.err.println("Wrong LOAD_DATA command");
                                continue;
                            } 
                            
                            String fileName = commands[1];
                            File file = new File(filePath + File.separator + fileName);
                            if(!file.exists() || !file.isFile()) {
                                System.err.println("File Not Exists");
                                client.close();
                                return;
                            }
                            FileInputStream fileStream = new FileInputStream(file);
                            OutputStream sendStream = client.getOutputStream();
                            sendStream.write(fileStream.readAllBytes());

                            //Done. Close all connections
                            sendStream.flush();
                            fileStream.close();
                            sendStream.close();
                            client.close();
                            return;
                        } 

                        else {
                            System.err.println("Unknown Command");
                            continue;
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } 
        }
        
    }

    public static void main(String[] args) {
        DStore dStore = new DStore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), String.valueOf(args[3]));
        dStore.start();
    }

}
