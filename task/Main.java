import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        ChatSystem system = new ChatSystem();
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println("\n1. Create User");
            System.out.println("2. Send Message");
            System.out.println("3. View Messages");
            System.out.println("4. Broadcast (Admin)");
            System.out.println("5. Exit");

            int choice = sc.nextInt();
            sc.nextLine();

            switch (choice) {
                case 1:
                    System.out.print("Enter name: ");
                    String name = sc.nextLine();

                    System.out.print("Is Admin? (y/n): ");
                    String isAdmin = sc.nextLine();

                    if (isAdmin.equalsIgnoreCase("y")) {
                        system.addUser(new Admin(name));
                    } else {
                        system.addUser(new User(name));
                    }

                    System.out.println("User created!");
                    break;

                case 2:
                    System.out.print("Sender name: ");
                    User sender = system.findUserByName(sc.nextLine());

                    System.out.print("Receiver name: ");
                    User receiver = system.findUserByName(sc.nextLine());

                    if (sender == null || receiver == null) {
                        System.out.println("User not found!");
                        break;
                    }

                    System.out.print("Message: ");
                    String msg = sc.nextLine();

                    system.sendMessage(sender, receiver, msg);
                    System.out.println("Message sent!");
                    break;

                case 3:
                    System.out.print("Enter user name: ");
                    User user = system.findUserByName(sc.nextLine());

                    if (user == null) {
                        System.out.println("User not found!");
                        break;
                    }

                    system.viewMessages(user);
                    break;

                case 4:
                    System.out.print("Admin name: ");
                    User admin = system.findUserByName(sc.nextLine());

                    if (admin == null || !admin.isAdmin()) {
                        System.out.println("Not an admin!");
                        break;
                    }

                    System.out.print("Message: ");
                    String content = sc.nextLine();

                    system.broadcast(admin, content);
                    System.out.println("Broadcast sent!");
                    break;

                case 5:
                    System.out.println("Goodbye!");
                    return;

                default:
                    System.out.println("Invalid choice!");
            }
        }
    }
}