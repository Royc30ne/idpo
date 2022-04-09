import java.io.BufferedReader;
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
        ServerSocket serverSocket = new ServerSocket(cport);

        while(true) {
            Socket client = serverSocket.accept();

            client.close();
            break;
        }
    }

    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),  Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.startController();
    }
}
