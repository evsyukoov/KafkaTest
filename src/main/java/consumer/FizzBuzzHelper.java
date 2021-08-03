package consumer;

public class FizzBuzzHelper {

    public static void print(String number) {
        String printNum = number;
        int check = Integer.parseInt(number);
        if (check % 3 == 0) {
            System.out.print("fizz");
            printNum = "";
        }
        if (check % 5 == 0) {
            System.out.print("buzz");
            printNum = "";
        }
        System.out.println(printNum);
    }
}
