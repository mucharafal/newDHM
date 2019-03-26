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

public class DistributedHashMap<K, V> extends ReceiverAdapter{

    private static String stringAddress = "230.100.200.11";
    private static String channelName = "synchro";
    
    private HashMap<K, V> map = new HashMap<>();
    private JChannel channel;

    public V getElement(K key){
        return map.get(key);
    }

    public boolean containsKey(K key){
        return map.containsKey(key);
    }

    public void addElement(K key, V value) throws Exception {
        MyMessage message = new MyMessage(RequestType.ADD, key, value);
        channel.send(new Message(null, message));
    }

    public void remove(K key) throws Exception {
        MyMessage message = new MyMessage(RequestType.REMOVE, key, null);
        channel.send(new Message(null, message));
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        System.out.println("Get state ; insides");
        ObjectOutputStream outputStream = new ObjectOutputStream(output);
        outputStream.writeObject(map);
        System.out.println("state get, map: " + map.size());
    }

    @Override
    public void setState(InputStream input) throws Exception {
        map = (HashMap<K, V>)new ObjectInputStream(input).readObject();
    }

    public void receive(Message msg){
        processMessage(msg);
    }

    public void receive(MessageBatch batch){
        for(Message msg: batch){
            processMessage(msg);
        }
        try {
            channel.getState(null, 12000, true);
        } catch (Exception e){
            e.printStackTrace();
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

    public DistributedHashMap() throws Exception{
        setupProperties();
        initializeChannel();
    }

    private void initializeChannel() throws Exception{
        UDP udp = new UDP();
        udp.setValue("mcast_group_addr", InetAddress.getByName(stringAddress));
        ProtocolStack stack = new ProtocolStack();
        channel = new JChannel(false);
        channel.setProtocolStack(stack);
        stack.addProtocol(udp)
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout",12000).setValue("interval",3000 ))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE())
                .addProtocol(new SEQUENCER())
                .addProtocol(new FLUSH());
        stack.init();

        channel.connect(channelName);

        channel.setReceiver(this);

        channel.getState(null, 12000, true);
    }

    private void setupProperties() throws Exception{
        System.setProperty("java.net.preferIPv4Stack","true");
    }
}
