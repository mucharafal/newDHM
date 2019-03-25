import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class DistributedMapClient {
    public static void main(String[] args) throws Exception{
        DistributedHashMap<String, Integer> map = new DistributedHashMap<>();
        Boolean play = true;
        Scanner reader = new Scanner(System.in);
        while(play){
            System.out.println("What to do? 1- Add, 2- Remove, 3- Get, 4- Check, 5- end");
            int command = reader.nextInt();
            reader.nextLine();
            String key;
            int value;
            switch(command){
                case 1:
                    System.out.println("Key?");
                    key = reader.nextLine();
                    System.out.println("Value?");
                    value = reader.nextInt();
                    reader.nextLine();
                    map.addElement(key, value);
                    break;
                case 2:
                    System.out.println("Key?");
                    key = reader.nextLine();
                    map.remove(key);
                    break;
                case 3:
                    System.out.println("Key?");
                    key = reader.nextLine();
                    System.out.println("Value: " + map.getElement(key));
                    break;
                case 4:
                    System.out.println("Key?");
                    key = reader.nextLine();
                    System.out.println("Value: " + map.containsKey(key));
                    break;
                case 5:
                    play = false;
            }

        }
    }
}
