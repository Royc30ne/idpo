import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class DStore {
    private int port;
    private final int cport;
    private final int timeout;
    private String file_folder;
    private String filePath;
    private boolean controllerConnected = false;

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
            
            new ControllerThread(controller).start();

            if (!controllerConnected) {
                return;
            } 
            
            try {
                ServerSocket serverSocket = new ServerSocket(port);
                while(true) {
                    Socket client = serverSocket.accept();
                    new ClientThread(client, controller).start();
                }

            } catch (Exception e) {
                e.printStackTrace();
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
        private Thread controllerThread;
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
                            String[] splits = readline.split(" ");
                            var command = splits[0].trim();

                            //COMMAND: JOIN
                            if(command.equals(Protocal.JOIN_SUCCESS_TOKEN)) {
                                controllerConnected = true;
                                System.out.println("Successfully build connection with Controller");
                            }
                        }

                } 
            } catch (IOException e1) {
                e1.printStackTrace();
                controllerConnected = false;
            }
        }

        public void start() {
            System.out.println("Controller Thread Start");
            controllerThread = new Thread(this, "Controller Thread");
            controllerThread.start();
        }

    }

    /**
     * Thread to process Client
     */
    class ClientThread implements Runnable {
        private Thread clientThread;
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
                        System.out.println("Client Command: " + command);
                        
                        //COMMAND: STORE
                        if(command.equals(Protocal.STORE_TOKEN)) {
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
                                break;
                            }

                            fos.flush();
                            fos.close();
                            client.close();
                            return;
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } 
        }

        public void start() {
            System.out.println("Client Thread Start.");
            clientThread = new Thread(this, "Client Thread");
            clientThread.start();
        }
        
    }

    public static void main(String[] args) {
        DStore dStore = new DStore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), String.valueOf(args[3]));
        dStore.start();
    }

}
