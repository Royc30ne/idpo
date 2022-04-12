import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class DStore {
    private int port;
    private final int cport;
    private final int timeout;
    private String file_folder;

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
        final String store_path = folder.getAbsolutePath();
        clearFolder(folder);

        try {
            System.out.println("DStore Port: " + port);
            Socket controller = new Socket(InetAddress.getByName("localhost"), cport);
            System.out.println("Controller Connected.\nPort: " + cport);
            
            new Thread(() -> {
                //Controller Operations
                try {
                    BufferedReader readController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
                    PrintWriter sendController = new PrintWriter(controller.getOutputStream(), true);
                
                    String readline = null;
                    
                    //Join Controller
                    sendController.println(Protocal.JOIN_TOKEN + " " + port);

                    //Process Operations
                    while(true) {
                        try {
                            readline = readController.readLine().trim();
                            
                            if(readline != null) {
                                String[] splits = readline.split(" ");
                                var command = splits[0].trim();
                                
                                if(command.equals(Protocal.JOIN_SUCCESS_TOKEN)) {
                                    System.out.println("Successfully build connection with Controller");
                                }

                            }
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    } 
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }).start();

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

    public static void main(String[] args) {
        DStore dStore = new DStore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), String.valueOf(args[3]));
        dStore.start();
    }

}
