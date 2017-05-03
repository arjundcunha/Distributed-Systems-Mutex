import java.net.*;
import java.io.*;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.math.BigInteger;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.*;

// Pair Class to hold the LRQ with Seq Number and ID
class Pair {
    int seq;
    int id;
    public Pair(int _seq, int _id)
    {
        this.seq = _seq;
        this.id = _id;
    }
    public void printit()
    {
       // System.out.println(String.valueOf(this.seq) + " " + String.valueOf(String.valueOf(this.id)))    ;
    }
}

// Custom class to sort the LRQ
class mySorter implements Comparator <Pair> {
    public int compare(Pair e1, Pair e2) {
        if(e1.seq < e2.seq)
            return -1;
        else if(e1.seq == e2.seq) {
            if(e1.id < e2.id)
                return -1;
            else if(e1.id == e2.id)
                return 0;
            else if(e1.id > e2.id)
                return 1;
        }
        return 1;
    }
}
public class Lodha {
    private static final SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
    private static String get_time_string() {
        return df.format(new Date());
    }
    public enum msgType {
        Request,
        Reply,
        Flush
    }
    // MEssage Format
    public static class message {
        public int target;
        public int source;
        public msgType type;
        public int id;
        public int seq_num;
        public message(int _target,int _source, msgType _type, int _id, int _seq) {
            target=_target;
            source=_source;
            type=_type;
            id = _id;
            seq_num = _seq;
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
        private static Pattern message_deserialize = Pattern.compile("(\\d+) (\\d+) (\\w+) (\\d+) (-?\\d+)");
        public sock_reader(Socket _endpoint,int other_proc_id,ArrayBlockingQueue<message> _msg_queue) {
            endpoint=_endpoint;
            msg_queue=_msg_queue;
            proc_id=other_proc_id;
        }
        public void run() {
            try {
                InputStream is=endpoint.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                //keep reading messages, deserializing them and pushing them into the message queue
                for (;;) {
                    String line=br.readLine();
                    if (line!=null) {
                        Matcher segments=message_deserialize.matcher(line);
                        if (segments.find()) {
                            int target = Integer.parseInt(segments.group(1));
                            int src = Integer.parseInt(segments.group(2));
                            msgType typ =  msgType.valueOf(segments.group(3));
                            int reqid = Integer.parseInt(segments.group(4));
                            int seqno = Integer.parseInt(segments.group(5));
                            message msg=new message(target,src,typ,reqid,seqno);
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
                        msg_queue.put(new message(0,proc_id,msgType.Request,0,0));
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
                    System.out.println(get_time_string() + " : Process "+ proc_id + " makes CS Request");
                    outbox.put(new message(proc_id,proc_id,msgType.Request,0,0));
                    inbox.take(); //waits to enter CS
                }
                catch (InterruptedException ie) {
                    ;
                }
                System.out.println(get_time_string() + " : Process "+ proc_id + " enters CS");
                unirand = engine.nextDouble();
                exprand = csdelay*(-Math.log(1.0-unirand));
                try {
                    Thread.sleep((long)(1000*exprand));
                    System.out.println(get_time_string() + " : Process "+ proc_id + " leaves CS");
                    outbox.put(new message(proc_id,proc_id,msgType.Reply,0,0));
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
        builder.append(String.valueOf(msg.type));
        builder.append(' ');
        builder.append(String.valueOf(msg.id));
        builder.append(' ');
        builder.append(String.valueOf(msg.seq_num));
        builder.append('\n');
        String output=builder.toString();
        os.write(output.getBytes("US-ASCII"));
        os.flush();
    }
    // Function to Check if the process can enter CS
    private static boolean checkExecuteCS(Boolean[] boo) throws IOException {
        int p;
        for (p = 0; p<boo.length; p++) {
            if (!boo[p]) {
                return false;
            }
        }
        return true;
    }

    // Once a function has exited CS, it print this
    private static void FinCS(HashMap<Integer,Integer> fwd_table,HashMap<Integer,Socket> endpoints,List<Pair> lrq,int _procID, ArrayList<Integer> boo) throws IOException {
        if (lrq.size() >=2) {
            System.out.println(get_time_string() + " : Process "+ lrq.get(0).id + " sends a Flush Message to "+lrq.get(1).id);
            message msg = new message(lrq.get(1).id , _procID, msgType.Flush , _procID , lrq.get(0).seq);
            Socket target=endpoints.get(fwd_table.get(lrq.get(1).id));
            send_msg(target,msg);
        }
        for (Integer p : boo) {
            System.out.println(get_time_string() + " : Process "+ lrq.get(0).id + " sends a Deffered Reply Message to "+p);
            message msg1 = new message(p, _procID, msgType.Reply, _procID, lrq.get(0).seq);
            Socket target1=endpoints.get(fwd_table.get(p));
            send_msg(target1,msg1);
        }
    }
    // Removing entries with lower priority
    private static boolean removePrio(List<Pair> lrq, message lm) throws IOException {
        if (lrq.get(0).seq < lm.seq_num)
            return true;
        else if (lrq.get(0).seq == lm.seq_num && lrq.get(0).id <= lm.id)
            return true;
        return false;
    }

    // Printing the Queue
    private static void printLRQ(List<Pair> lrq) throws IOException {
        for (int i = 0;i<lrq.size() ;i++ ) {
            lrq.get(i).printit();
        }
    }
    public static void main(String[] args) {
        int proc_id=Integer.parseInt(args[0]);
        //Code to read in the input parameters
        int num_procs, numReq;
        double meanDelay, csDelay;
        BufferedReader inp_params_reader=null;
        try
        {
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
            try
            {
                inp_params_reader.close();
            }
            catch (IOException ioe)
            {
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
                int parent=0;
                for (int i=0;i!=num_procs;++i) {
                    long is_there=span_tree.get(i).stream().filter(c->c.equals(new Integer(proc_id))).count();
                    if (is_there>0) {
                        parent=i+1;
                        break;
                    }
                }
                for (int i=0;i!=num_procs;++i)
                {
                    if (i!=proc_id-1 && (fwd_table.containsKey(i+1)==false))
                    {
                        fwd_table.put(i+1,parent);
                    }
                }
            }
            server listener = new server((int)portnum,(int)pending_ip);
            Thread t = new Thread(listener);
            t.start();
            System.out.println("Read input fully.");
            String line = linereader.readLine();
            if (!line.equals("Go")) {
                System.out.println("Aborting");
                return;
            }
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
            // All Relevant Datastucture
            int mySequenceNumber = 0;
            Boolean[] rv = new Boolean[num_procs];
            Arrays.fill(rv, Boolean.FALSE);
            int highestSequenceNumberSeen = 0;
            List<Pair> lrq = new ArrayList<Pair>();
            ArrayList<Integer> deferredReqID = new ArrayList<Integer>();
            lrq.clear();
            deferredReqID.clear();
            int lastSatisfied = -1;
            int killThemAll = 0;
            ArrayBlockingQueue<Integer> req_inbox=new ArrayBlockingQueue<Integer>(5);
            Thread csthread = new Thread(new CSRequestor(incoming,req_inbox,numReq,meanDelay,csDelay,proc_id));
            csthread.start();
            long startTime = 0;
            long duration = 0;
            int total_messages = 0;
            
            for (;;) {
            //    System.out.println("Total number of messages are : " + total_messages);
            //    System.out.println("Total Time to enter CS is  : " + duration);

                if (killThemAll >= numReq )
                    break;
                // Request to Enter CS
                message local_message=incoming.take();
                if (local_message.source == proc_id) {
                    if (local_message.type == msgType.Request) {
                        printLRQ(lrq);
                        mySequenceNumber = highestSequenceNumberSeen + 1;
                       // System.out.println(mySequenceNumber + " " + proc_id);
                        lrq.add(new Pair(mySequenceNumber,proc_id));
                        Collections.sort(lrq, new mySorter());
                        rv[proc_id-1] = true;
                        int p;
                        // Send request to all processes
                        for (p = 1; p <= num_procs; p++) {
                            if (p!=proc_id) {
                                message msg = new message(p,proc_id,msgType.Request,proc_id,mySequenceNumber);
                                System.out.println(get_time_string() + " : Process "+ proc_id + " sends CS Request to "+p);
                                Socket target=endpoints.get(fwd_table.get(p));
                                send_msg(target,msg);
                            }
                        }
                        startTime = System.currentTimeMillis();
                    }
                    // Exiting CS : Go to FinCS
                    else if(local_message.type == msgType.Reply) {
                        long elasped = System.currentTimeMillis() - startTime;
                        //System.out.println(elasped);
                        duration += elasped;
                        total_messages++;
                        total_messages += deferredReqID.size();
                        FinCS(fwd_table,endpoints,lrq,proc_id, deferredReqID);
                        lrq.remove(0);
                        rv[proc_id-1] = false;
                        deferredReqID.clear();
                    }
                }
                // Recieve A message from another process
                else if (local_message.target==proc_id) {
                    // If it is a request
                    if (local_message.type == msgType.Request) {
                        System.out.println(get_time_string() + " : Process "+ proc_id + " gets CS Request from "+local_message.source);
                        highestSequenceNumberSeen = Math.max(highestSequenceNumberSeen, local_message.seq_num);
                        // If I am requesting
                        if (rv[proc_id-1]) {
                            // If the source has already requested
                            // Defers its request
                            if (rv[local_message.source-1]) {
                                System.out.println(get_time_string() + " : Process "+ proc_id + " deffers CS Request from "+local_message.source);
                                deferredReqID.add(local_message.source);
                            }
                            else {
                                // Add to self LRQ
                                lrq.add(new Pair(local_message.seq_num,local_message.source));
                                Collections.sort(lrq, new mySorter());
                                rv[local_message.source-1] = true;
                                // If I can enter CS
                                if (checkExecuteCS(rv) && lrq.get(0).id == proc_id) {
                                    lastSatisfied = lrq.get(0).seq;
                                    // Enter the CS
                                    req_inbox.put(0);
                                    // Exit the CS
                                }
                            }
                            printLRQ(lrq);
                        }
                        else { // Send Reply
                            System.out.println(get_time_string() + " : Process "+ proc_id + " sends Reply to "+local_message.source);
                            message msg = new message(local_message.source,proc_id, msgType.Reply, proc_id,lastSatisfied);
                            Socket target=endpoints.get(fwd_table.get(local_message.source));
                            send_msg(target,msg);
                            printLRQ(lrq);
                        }
                    }
                    // Recieve a Reply
                    else if (local_message.type == msgType.Reply) {
                        System.out.println(get_time_string() + " : Process "+ proc_id + " gets Reply from "+local_message.source);
                        rv[local_message.source-1] = true;
                        while(true && lrq.size()>0) {
                            if (removePrio(lrq,local_message)) {
                                lrq.remove(0);
                            }
                            else
                                break;
                        }
                        // Check if i can enter CS
                        if (checkExecuteCS(rv) && lrq.get(0).id == proc_id) {
                            lastSatisfied = lrq.get(0).seq;
                            // Enter the CS
                            req_inbox.put(0);
                            // Exit the CS
                        }
                        printLRQ(lrq);
                    }
                    else {     // Recieve Flush
                        System.out.println(get_time_string() + " : Process "+ proc_id + " gets Flush from "+local_message.source);
                        req_inbox.put(0);
                        rv[local_message.source-1] = true;
                        while(true && lrq.size()>0) {
                            if (removePrio(lrq,local_message)) {
                                lrq.remove(0);
                            }
                            else
                                break;
                        }
                        // Check if I can Enter CS
                        if (checkExecuteCS(rv) && lrq.get(0).id == proc_id) {
                            lastSatisfied = lrq.get(0).seq;
                            // Enter the CS
                            req_inbox.put(0);
                            // Exit the CS
                        }
                    }
                }
                // Need to Forward Message
                else {
           //         System.out.format("Forwarding message from %d to %d\n",local_message.source,local_message.target);
                    send_msg(endpoints.get(fwd_table.get(local_message.target)), local_message);
                }
            }
            for (int i=0;i!=neighbours.size();++i) {
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
