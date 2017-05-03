import java.net.*;
import java.io.*;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.*;
import java.util.Iterator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.math.BigInteger;
import java.util.Date;
import java.text.SimpleDateFormat;
public class Singhal {
    private static final SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
    private static String get_time_string() {
        return df.format(new Date());
    }
    public enum msgType {
        Request,
        Reply
    }
    private static class message {
        public int target;
        public int source;
        public msgType typ;
        public int timestamp;
        public message(int _target,int _source, msgType _typ, int _timestamp) {
            target=_target;
            source=_source;
            typ=_typ;
            timestamp=_timestamp;
        }
    }
    private static class server implements Runnable {
        public HashMap<Integer,Socket> endpoints;
        private int port;
        private int num_connects;
        public server(int server_port,int _num_connects) {
            endpoints=new HashMap<Integer,Socket>();
            port=server_port;
            num_connects=_num_connects;
        }
        public void run() {
            try {
                if (num_connects!=0) {
                    ServerSocket service = new ServerSocket(port);
                    for (int i=0;i!=num_connects;++i) {
                        Socket endpoint = service.accept();
                        InputStream is = endpoint.getInputStream();
                        int other_id=is.read();
                        endpoints.put(other_id,endpoint);
                    }
                    service.close();
                }
            }
            catch(IOException ioe) {
                ;
            }
            finally {
                ;
            }
        }
    }
    private static class sock_reader implements Runnable {
        private Socket endpoint;
        private ArrayBlockingQueue<message> msg_queue;
        private int proc_id;
        private static Pattern message_deserialize = Pattern.compile("(\\d+) (\\d+) (\\w+) (\\d+)");
        public sock_reader(Socket _endpoint,int other_proc_id,ArrayBlockingQueue<message> _msg_queue) {
            endpoint=_endpoint;
            msg_queue=_msg_queue;
            proc_id=other_proc_id;
        }
        public void run() {
            try {
                InputStream is=endpoint.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                for (;;){ //keep reading messages, deserializing them and pushing them into the message queue
                    String line=br.readLine();
                    if (line!=null) {
                        Matcher segments=message_deserialize.matcher(line);
                        if (segments.find()) {
                            message msg=new message(Integer.parseInt(segments.group(1)),Integer.parseInt(segments.group(2)),msgType.valueOf(segments.group(3)),Integer.parseInt(segments.group(4)));
                            msg_queue.put(msg);
                        }
                    }
                    else { //socket has been closed
                        break;
                    }
                }
            }
            catch (IOException ioe) {
                ;
            }
            catch (InterruptedException ie) {
                ;
            }
            finally {
                try {
                    endpoint.close();
                }
                catch (IOException ioe) {
                    ;
                }
                finally {
                    try {
                        msg_queue.put(new message(0,0,msgType.Request,0));
                    }
                    catch (InterruptedException ie) {
                        ;
                    }
                }
            }
        }
    }
    private static class CSRequestor implements Runnable {
        private ArrayBlockingQueue<message> outbox;
        private ArrayBlockingQueue<Integer> inbox;
        private int reqcnt;
        double meandelay;
        double csdelay;
        int proc_id;
        public CSRequestor(ArrayBlockingQueue<message> _outbox,ArrayBlockingQueue<Integer> _inbox,int _reqcnt,double _meandelay,double _csdelay,int _proc_id) {
            outbox=_outbox;
            inbox=_inbox;
            reqcnt=_reqcnt;
            meandelay=_meandelay;
            csdelay=_csdelay;
            proc_id=_proc_id;
        }
        public void run() {
            Random engine = new Random();
            for (int i=0;i!=reqcnt;++i) {
                double unirand = engine.nextDouble();
                double exprand = meandelay*(-Math.log(1.0-unirand));
                try {
                    Thread.sleep((long)(1000*exprand));
                    System.out.println("Making CS Request at "+get_time_string());
                    outbox.put(new message(proc_id,proc_id,msgType.Request,0));
                    inbox.take(); //waits to enter CS
                }
                catch (InterruptedException ie) {
                    ;
                }
                System.out.println("Entering CS at "+get_time_string());
                unirand = engine.nextDouble();
                exprand = csdelay*(-Math.log(1.0-unirand));
                try {
                    Thread.sleep((long)(1000*exprand));
                    System.out.println("Leaving CS at "+get_time_string());
                    outbox.put(new message(proc_id,proc_id,msgType.Reply,0));
                }
                catch (InterruptedException ie) {
                    ;
                }
            }
        }
    }
    private static void send_msg(Socket target,message msg) throws IOException {
        OutputStream os=target.getOutputStream();
        StringBuilder builder=new StringBuilder();
        builder.append(msg.target);
        builder.append(' ');
        builder.append(msg.source);
        builder.append(' ');
        builder.append(msg.typ.toString());
        builder.append(' ');
        builder.append(String.valueOf(msg.timestamp));
        builder.append('\n');
        String output=builder.toString();
        os.write(output.getBytes("US-ASCII"));
        os.flush();
    }
    public static void main(String[] args) {
        int proc_id=Integer.parseInt(args[0]);
        //Code to read in the input parameters
        int num_procs, numReq;
        double meanDelay,csDelay;
        BufferedReader inp_params_reader=null;
        try {
            inp_params_reader = new BufferedReader(new FileReader("inp-params.txt"));
            String line = inp_params_reader.readLine();
            String[] parts = line.trim().split("\\s+");
            num_procs=Integer.parseInt(parts[0]);
            numReq=Integer.parseInt(parts[1]);
            meanDelay=Double.parseDouble(parts[2]);
            csDelay=Double.parseDouble(parts[3]);
        }
        catch(IOException ioe) {
            ;
            return;
        }
        finally {
            try {
                inp_params_reader.close();
            }
            catch (IOException ioe) {
                ;
            }
        }
        //code to read in topology of the graph
        Pattern get_host = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");
        HashMap<Integer,InetSocketAddress> pending_port = new HashMap<Integer,InetSocketAddress>();
        ArrayList<ArrayList<Integer>> adjacency_list=new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> span_tree=new ArrayList<ArrayList<Integer>>();
        ArrayList<Boolean> init_active=new ArrayList<Boolean>();
        int root_pid;
        int portnum=0;
        BufferedReader topology_reader=null;
        try {
            topology_reader = new BufferedReader(new FileReader("topology.txt"));
            //String line = topology_reader.readLine(); //first line just has n, ignore it
            for (int i=0;i!=num_procs;++i) { //read ip port bindings
                String line=topology_reader.readLine();
                String[] parts = line.trim().split("\\s+"); //proc_id,-,IP:port
                int other_pid=Integer.parseInt(parts[0]);
                Matcher ip_with_port = get_host.matcher(parts[2]);
                ip_with_port.find();
                pending_port.put(other_pid,new InetSocketAddress(ip_with_port.group(1),Integer.parseInt(ip_with_port.group(2))));
                if (i==proc_id-1) {
                    portnum=Integer.parseInt(ip_with_port.group(2));
                }
            }
            for (int i=0;i!=num_procs;++i) { //read adjacency list
                String line=topology_reader.readLine();
                String[] parts = line.trim().split("\\s+"); //node, neighbours...
                ArrayList<Integer> new_neighbours=new ArrayList<Integer>();
                for (int j=1;j!=parts.length;++j) {
                    new_neighbours.add(Integer.parseInt(parts[j]));
                }
                adjacency_list.add(new_neighbours);
            }
            //read one line of tree topology to find the root
            String line=topology_reader.readLine();
            String[] parts = line.trim().split("\\s+"); //root node, root's children
            root_pid=Integer.parseInt(parts[0]);
            ArrayList<Integer> root_children=new ArrayList<Integer>();
            for (int j=1;j!=parts.length;++j) {
                root_children.add(Integer.parseInt(parts[j]));
            }
            span_tree.add(root_children);
            //read in rest of spanning tree
            for (int i=1;i!=num_procs;++i) {
                line=topology_reader.readLine();
                parts = line.trim().split("\\s+"); //node, neighbours...
                ArrayList<Integer> children=new ArrayList<Integer>();
                for (int j=1;j!=parts.length;++j) {
                    children.add(Integer.parseInt(parts[j]));
                }
                span_tree.add(children);
            }
        }
        catch(IOException ioe) {
            ;
            return;
        }
        finally {
            try {
                topology_reader.close();
            }
            catch (IOException ioe) {
                ;
            }
        }
        BufferedReader linereader = new BufferedReader(new InputStreamReader(System.in));
        HashMap<Integer,Socket> endpoints = new HashMap<Integer,Socket>();
        HashMap<Integer,Integer> fwd_table = new HashMap<Integer,Integer>();
        ArrayList<Integer> neighbours=adjacency_list.get(proc_id-1);
        int num_neighbours=neighbours.size();
        long pending_ip=neighbours.stream().filter(c -> (c.compareTo(new Integer(proc_id))>0)).count();
        ArrayBlockingQueue<message> incoming=new ArrayBlockingQueue<message>(20);
        try {
            //Code to construct forwarding table
            { //perform DFS of this node's children to construct forwarding table for those nodes, route rest of the messages to its parent
                ArrayList<Integer> span_children = span_tree.get(proc_id-1);
                for (int i=0;i!=span_children.size();++i) {
                    Stack<Integer> to_visit = new Stack<Integer>();
                    int child_id=span_children.get(i);
                    to_visit.push(span_children.get(i));
                    while (!to_visit.empty()) {
                        int value=to_visit.pop();
                        fwd_table.put(value,child_id);
                        ArrayList<Integer> local_children = span_tree.get(value-1);
                        for (int j=0;j!=local_children.size();++j) {
                            to_visit.push(local_children.get(j));
                        }
                    }
                }
                if (root_pid!=proc_id) {
                    //need to find parent of this current node
                    int parent=0;
                    for (int i=0;i!=num_procs;++i) {
                        long is_there=span_tree.get(i).stream().filter(c->c.equals(new Integer(proc_id))).count();
                        if (is_there>0) {
                            parent=i+1;
                            break;
                        }
                    }
                    for (int i=0;i!=num_procs;++i) {
                        if (i!=proc_id-1 && (fwd_table.containsKey(i+1)==false)) {
                            fwd_table.put(i+1,parent);
                        }
                    }
                }
            }
            for (Map.Entry<Integer,Integer> entry : fwd_table.entrySet()) {
                System.out.format("%d %d\n",entry.getKey(),entry.getValue());
            }
            /*Need to spin off ServerSocket here*/
            server listener = new server((int)portnum,(int)pending_ip);
            Thread t = new Thread(listener);
            t.start();
            System.out.println("Read input fully.");
            String line = linereader.readLine();
            if (!line.equals("Go")) {
                System.out.println("Aborting");
                return;
            }
            //now need to connect to the other nodes
            for (int i=0;i!=num_neighbours;++i) {
                int other_id=neighbours.get(i);
                if (other_id<proc_id) {
                    Socket sock=new Socket();
                    sock.connect(pending_port.get(other_id));
                    endpoints.put(other_id,sock);
                    OutputStream os = sock.getOutputStream();
                    os.write(proc_id);
                }
            }
            t.join();
            //merge the two sets of sockets
            for (Map.Entry<Integer,Socket> entry : listener.endpoints.entrySet()) {
                endpoints.put(entry.getKey(),entry.getValue());
            }
            //can start running the distributed algorithm
            int running_total=0;
            Random generator = new Random();
            System.out.println("Connections established!");
            line = linereader.readLine();
            if (!line.equals("Go")) {
                System.out.println("Aborting");
                return;
            }
            Thread readerthreads[] = new Thread[num_neighbours];
            for (int i=0;i!=num_neighbours;++i) {
                readerthreads[i] = new Thread(new sock_reader(endpoints.get(neighbours.get(i)),neighbours.get(i),incoming));
                readerthreads[i].start();
            }
            HashSet<Integer> requestSet = new HashSet<Integer>();
            for (int i=1;i!=proc_id;++i) {
                requestSet.add(i);
            }
            HashSet<Integer> informSet = new HashSet<Integer>();
            int clock=0;
            boolean requesting=false, executing=false;
            int my_priority=Integer.MAX_VALUE;
            ArrayBlockingQueue<Integer> req_inbox=new ArrayBlockingQueue<Integer>(5);
            Thread csthread = new Thread(new CSRequestor(incoming,req_inbox,numReq,meanDelay,csDelay,proc_id));
            csthread.start();
            for (;;) {
                message msg = incoming.take();
                if (msg.source==proc_id) { //self CS msg
                    if (msg.typ==msgType.Request) { //self CS request
                        requesting=true;
                        ++clock;
                        my_priority=clock;
                        for (Integer iter : requestSet) {
                            System.out.format("Asking %d for permission to enter CS.\n",iter);
                            send_msg(endpoints.get(fwd_table.get(iter)),new message(iter,proc_id,msgType.Request,clock));
                        }
                    }
                    else { //self CS release
                        executing=false;
                        for (Integer iter : informSet) {
                            System.out.format("Telling %d about CS exit.\n",iter);
                            send_msg(endpoints.get(fwd_table.get(iter)),new message(iter,proc_id,msgType.Reply,clock));
                            requestSet.add(iter);
                        }
                        informSet.clear();
                    }
                }
                else if (msg.source==0) { //signal to close connections
                    for (Map.Entry<Integer,Socket> term : endpoints.entrySet()) {
                        try {
                            term.getValue().close();
                        }
                        catch (IOException ioe) {
                            ;
                        }
                    }
                    break;
                }
                else {
                    if (msg.target==proc_id) {
                        if (msg.typ==msgType.Request) {
                            clock=Math.max(clock,msg.timestamp);
                            if (requesting==true) {
                                if (my_priority<msg.timestamp||(my_priority==msg.timestamp&&msg.source<proc_id)) {
                                    informSet.add(msg.source);
                                }
                                else {
                                    System.out.format("Granting permission to %d to enter CS.\n",msg.source);
                                    send_msg(endpoints.get(fwd_table.get(msg.source)),new message(msg.source,proc_id,msgType.Reply,clock));
                                    if (!requestSet.contains(new Integer(msg.source))) {
                                        requestSet.add(new Integer(msg.source));
                                        System.out.format("Requesting permission from %d to enter CS.\n",msg.source);
                                        send_msg(endpoints.get(fwd_table.get(msg.source)),new message(msg.source,proc_id,msgType.Request,clock));
                                    }
                                }
                            }
                            else if (executing==true) {
                                informSet.add(msg.source);
                            }
                            else {
                                requestSet.add(msg.source);
                                System.out.format("Granting permission to %d to enter CS.\n",msg.source);
                                send_msg(endpoints.get(fwd_table.get(msg.source)),new message(msg.source,proc_id,msgType.Reply,clock));
                            }
                        }
                        else {
                            clock=Math.max(clock,msg.timestamp);
                            requestSet.remove(new Integer(msg.source));
                        }
                    }
                    else {
                        System.out.format("Forwarding message from %d to %d\n",msg.source,msg.target);
                        send_msg(endpoints.get(fwd_table.get(msg.target)),new message(msg.target,msg.source,msg.typ,msg.timestamp));
                    }
                }
                if (requesting==true&&requestSet.isEmpty()) { //have received permission from everyone necessary
                    requesting=false;
                    executing=true;
                    req_inbox.put(0);
                }
            }
            csthread.join();
            for (int i=0;i!=num_neighbours;++i) {
                readerthreads[i].join();
            }
        }
        catch (IOException ioe) {
            ;
        }
        catch (InterruptedException ie) {
            ;
        }
    }
}
