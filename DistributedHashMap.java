import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

public class DistributedHashMap<K, V> {
    private static String groupAddressString = "230.100.200.9";
    private static String groupSynchronizationAddressString = "230.100.200.10";
    private static String channelName = "seleythen";
    private static String channelSynchronization = "synchro";
    private InetAddress groupAddress;
    private InetAddress groupSynchronizationAddress;

    private class Server extends ReceiverAdapter {
        private HashMap<K, V> map = null;
        private JChannel channel;
        private JChannel synchronizationChannel;
        Server() throws Exception{
            channel = initializeChannel(groupAddress);
            channel.setReceiver(this);
            channel.connect(channelName);

            synchronizationChannel = initializeChannel(groupSynchronizationAddress);
            synchronizationChannel.connect(channelSynchronization);

            System.out.println("Before getState");
            if(synchronizationChannel.getView().size() != 1){
                synchronizationChannel.getState();
            }
            System.out.println("After getState");
            if(map == null){
                map = new HashMap<>();
            }
        }

        V getElement(K key){
            return map.get(key);
        }

        boolean containsKey(K key){
            return map.containsKey(key);
        }

        public void getState(OutputStream output) throws Exception {
            System.out.println("state get, map: " + map.size());

            output.write(Util.objectToByteBuffer(map));
        }

        public void setState(InputStream input) throws Exception {
            System.out.println("state set, map: " + map.size());
            map = (HashMap<K, V>)Util.objectFromStream(new DataInputStream(input));
        }

        public void receive(Message msg){
            processMessage(msg);
        }

        public void receive(MessageBatch batch){
            for(Message msg: batch){
                processMessage(msg);
            }
        }

        private void processMessage(Message msg){
            System.out.println("Received message");
            MyMessage<K, V> message;
            try {
                message = (MyMessage<K, V>)msg.getObject();
                switch (message.type){
                    case ADD:
                        map.put(message.key, message.value);
                        break;
                    case REMOVE:
                        map.remove(message.key);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void viewAccepted(View view){
            if(view instanceof MergeView){
                MergeView tmp = (MergeView)view;
                List<View> subgroups = tmp.getSubgroups();
                Address myAddress = channel.getAddress();
                if(!subgroups.get(0).containsMember(myAddress)){
                    channel.getState();
                }
            }
        }
    }
    private class Client{
        JChannel channel;
        Client() throws Exception {
            channel = initializeChannel(groupAddress);
            channel.connect(channelName);
        }

        void add(K key, V value) throws Exception {
            MyMessage message = new MyMessage(RequestType.ADD, key, value);
            channel.send(new Message(null, message));
        }

        void remove(K key) throws Exception {
            MyMessage message = new MyMessage(RequestType.REMOVE, key, null);
            channel.send(new Message(null, message));
        }
    }

    private Client client;
    private Server server;

    public DistributedHashMap() throws Exception{
        setupProperties();
        client = new Client();
        server = new Server();
    }
    public void addElement(K key, V value) throws Exception{
        client.add(key, value);
    }

    public V getElement(K key){
        return server.getElement(key);
    }

    public boolean containsKey(K key){
        return server.containsKey(key);
    }

    public void remove(K key) throws Exception{
        client.remove(key);
    }

    private JChannel initializeChannel(InetAddress address) throws Exception{
        Protocol[] protocolStack = {new UDP().setValue("mcast_group_addr", address),
                new PING(),
                new MERGE3(),
                new FD_SOCK(),
                new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000),
                new VERIFY_SUSPECT(),
                new BARRIER(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS(),
                new UFC(),
                new MFC(),
                new FRAG2(),
                new FLUSH(),
                new STATE()};
        UDP udp = new UDP();
        udp.setValue("mcast_group_addr", address);
        ProtocolStack stack = new ProtocolStack();
        JChannel channel = new JChannel(false);
        channel.setProtocolStack(stack);
        stack.addProtocol(udp)
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval",3000 ))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2());
        stack.init();
        return channel;
    }

    private void setupProperties() throws Exception{
        System.setProperty("java.net.preferIPv4Stack","true");
        groupAddress = InetAddress.getByName(groupAddressString);
        groupSynchronizationAddress = InetAddress.getByName(groupSynchronizationAddressString);
    }
}
