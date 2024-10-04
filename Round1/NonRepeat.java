/*
Find the First Non-Repeating Character
Write a program to find the first non-repeating character in a string. For input "swiss", the output
should be "w". You cannot use any built-in string or character frequency counting functions.

Instructions: Implement manual string traversal and counting logic to solve the problem.
*/

import java.util.Scanner;

class NonRepeat {
    void non_repeat_character(String str) {
        int n = str.length();
        int[] count = new int[256];
        for (int i = 0; i < n; i++) {
            count[str.charAt(i)]++;
        }
        for (int i = 0; i < n; i++) {
            if (count[str.charAt(i)] == 1) {
                System.out.println("First Non-Repeating Character: " + str.charAt(i));
                return;
            }
        }
        System.out.println("No Non-Repeating Character found.");
    }

    public static void main(String args[]) {
        Repeat r = new Repeat();
        Scanner s = new Scanner(System.in);
        System.out.print("Enter the string: ");
        String str = s.nextLine();
        r.non_repeat_character(str);
    }
}
